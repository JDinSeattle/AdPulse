package io.adpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class CleanJob {
    public static final OutputTag<String> QUALITY = new OutputTag<String>("quality-lineage-v1") {};
    public static final OutputTag<String> QUARANTINE = new OutputTag<String>("quarantine-v1") {};
    public static ObjectNode quality(JsonNode packet, Rules rules, String disposition, String code) {
        ObjectNode q = Json.object();
        q.put("record_type", "quality"); q.put("receipt_id", packet.path("receipt_id").asText());
        q.put("event_id", packet.path("event").path("event_id").asText("unknown"));
        q.put("dataset", "raw_events"); q.put("rule_id", "event-contract-v1");
        q.put("rule_version", rules.ruleVersion); q.put("disposition", disposition); q.put("error_code", code);
        q.put("received_at", packet.path("received_at").asLong()); q.set("sample_ref", packet.path("_source"));
        return q;
    }
    public static class Validate extends ProcessFunction<String, String> {
        private final Rules rules; private final String schemaText; private transient JsonSchema schema;
        public Validate(Rules rules, String schemaText) { this.rules = rules; this.schemaText = schemaText; }
        @Override public void open(OpenContext context) {
            schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(schemaText);
        }
        @Override public void processElement(String input, Context ctx, Collector<String> out) {
            JsonNode packet = Json.read(input), event = packet.path("event");
            if (event.isObject()) {
                var mutable = (ObjectNode) event;
                Json.read(rules.json).path("field_mappings").fields().forEachRemaining(mapping -> {
                    if (mutable.has(mapping.getKey()) && !mutable.has(mapping.getValue().asText()))
                        mutable.set(mapping.getValue().asText(), mutable.remove(mapping.getKey()));
                });
                mutable.put("received_at", packet.path("received_at").asLong());
            }
            var errors = schema.validate(event);
            if (!errors.isEmpty()) {
                ObjectNode q = quality(packet, rules, "quarantined", "SCHEMA_INVALID");
                q.put("detail", errors.iterator().next().getMessage());
                ctx.output(QUALITY, q.toString()); q.set("raw", packet); ctx.output(QUARANTINE, q.toString()); return;
            }
            JsonNode config = Json.read(rules.json);
            boolean versionAllowed = false;
            for (var version : config.path("schema_versions")) versionAllowed |= version.asInt() == event.path("schema_version").asInt();
            if (!versionAllowed) {
                ObjectNode q = quality(packet, rules, "quarantined", "SCHEMA_VERSION_DISABLED");
                ctx.output(QUALITY, q.toString()); q.set("raw", packet); ctx.output(QUARANTINE, q.toString()); return;
            }
            boolean filtered = event.path("event_time").asLong() < config.path("effective_from").asLong();
            for (var region : config.path("filter_regions")) filtered |= region.asText().equals(event.path("region").asText());
            if (filtered) { ctx.output(QUALITY, quality(packet, rules, "filtered", "RULE_FILTERED").toString()); return; }
            ((ObjectNode) event).put("_receipt_id", packet.path("receipt_id").asText());
            ((ObjectNode) event).set("_source", packet.path("_source"));
            ((ObjectNode) event).put("rule_version", rules.ruleVersion);
            out.collect(event.toString());
        }
    }
    public static class Deduplicate extends KeyedProcessFunction<String, String, String> {
        private final Rules rules; private final boolean business;
        private transient ValueState<Long> expires;
        public Deduplicate(Rules rules, boolean business) { this.rules = rules; this.business = business; }
        @Override public void open(OpenContext ctx) {
            expires = getRuntimeContext().getState(new ValueStateDescriptor<>("expires-processing-v1", Long.class));
        }
        @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode e = Json.read(value);
            ObjectNode packet = Json.object(); packet.set("event", e); packet.set("_source", e.path("_source"));
            packet.put("receipt_id", e.path("_receipt_id").asText()); packet.put("received_at", e.path("received_at").asLong());
            long now = ctx.timerService().currentProcessingTime();
            Long expiry = expires.value();
            if (expiry != null && now < expiry) {
                ctx.output(QUALITY, quality(packet, rules, "duplicate", business ? "DUPLICATE_BUSINESS_KEY" : "DUPLICATE_EVENT_ID").toString());
                return;
            }
            expires.update(now + rules.dedup); ctx.timerService().registerProcessingTimeTimer(now + rules.dedup);
            if (business) ctx.output(QUALITY, quality(packet, rules, "cleaned", "OK").toString());
            out.collect(value);
        }
        @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (expires.value() != null && timestamp >= expires.value()) expires.clear();
        }
    }
    public static void main(String[] args) throws Exception {
        String contracts = RuntimeConfig.env("CONTRACTS_DIR", "/opt/adpulse/contracts");
        Rules rules = Rules.load(contracts, RuntimeConfig.env("RULES_FILE", "rules-v1.json"));
        var env = RuntimeConfig.environment("clean-" + RuntimeConfig.release());
        var raw = env.fromSource(RuntimeConfig.source(RuntimeConfig.topic("raw"), "adpulse-clean-" + RuntimeConfig.release(), true),
            WatermarkStrategy.noWatermarks(), "raw-kafka-v1").uid("raw-kafka-v1");
        var valid = raw.process(new Validate(rules, Files.readString(Path.of(contracts, "event.schema.json"))))
            .name("contract-validation").uid("contract-validation-v1");
        var byId = valid.keyBy(s -> { var e = Json.read(s); return Json.key(Json.text(e, "advertiser_id"), Json.text(e, "app_id"), Json.text(e, "event_id")); })
            .process(new Deduplicate(rules, false)).uid("event-id-dedup-v1");
        var cleaned = byId.keyBy(s -> Json.business(Json.read(s))).process(new Deduplicate(rules, true)).uid("business-dedup-v1");
        // SQL owns the standardized projections and transport-latency derivation.
        var table = StreamTableEnvironment.create(env);
        table.createTemporaryView("validated", table.fromDataStream(cleaned).as("payload"));
        var normalized = table.sqlQuery("SELECT payload, "
            + "CAST(JSON_VALUE(payload, '$.event_time') AS BIGINT) AS event_ts, "
            + "CAST(JSON_VALUE(payload, '$.received_at') AS BIGINT) - CAST(JSON_VALUE(payload, '$.event_time') AS BIGINT) AS transport_delay_ms "
            + "FROM validated");
        table.toDataStream(normalized).map((Row row) -> {
            ObjectNode event = (ObjectNode) Json.read((String) row.getField(0));
            event.put("event_time", (Long) row.getField(1)); event.put("transport_delay_ms", (Long) row.getField(2));
            return event.toString();
        }).returns(String.class).uid("sql-standard-output-v1")
            .sinkTo(RuntimeConfig.sink(RuntimeConfig.topic("clean"), "clean", "event_id")).uid("clean-kafka-sink-v1");
        valid.getSideOutput(QUALITY).union(byId.getSideOutput(QUALITY), cleaned.getSideOutput(QUALITY))
            .sinkTo(RuntimeConfig.sink(RuntimeConfig.topic("quality"), "quality", "receipt_id")).uid("quality-kafka-sink-v1");
        valid.getSideOutput(QUARANTINE).sinkTo(RuntimeConfig.sink(RuntimeConfig.topic("quarantine"), "quarantine", "receipt_id")).uid("quarantine-kafka-sink-v1");
        env.execute("AdPulse · cleaning and quality · " + rules.ruleVersion);
    }
}
