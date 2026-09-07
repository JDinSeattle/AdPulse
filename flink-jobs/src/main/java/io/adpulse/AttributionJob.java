package io.adpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class AttributionJob {
    public static final OutputTag<String> METRICS = new OutputTag<String>("metric-contributions-v1") {};
    public static final OutputTag<String> SIGNALS = new OutputTag<String>("attribution-quality-v1") {};
    public static final MapStateDescriptor<String, String> DIMENSIONS = new MapStateDescriptor<>("campaign-history-json-v1", String.class, String.class);
    public static String signal(JsonNode event, String reason, Rules rules) {
        var s = Json.object(); s.put("record_type", "quality"); s.put("receipt_id", event.path("_receipt_id").asText());
        s.put("event_id", event.path("event_id").asText()); s.put("dataset", "attribution");
        s.put("rule_id", "attribution-v1"); s.put("error_code", reason); s.put("disposition", "signal");
        s.put("rule_version", rules.ruleVersion); s.set("sample_ref", event.path("_source"));
        s.put("received_at", event.path("received_at").asLong()); return s.toString();
    }
    public static class ExposureJoin extends KeyedProcessFunction<String, String, String> {
        private final Rules rules;
        private transient ValueState<String> impression;
        private transient MapState<String, String> waiting;
        public ExposureJoin(Rules rules) { this.rules = rules; }
        @Override public void open(OpenContext ctx) {
            impression = getRuntimeContext().getState(new ValueStateDescriptor<>("impression-json-v1", String.class));
            waiting = getRuntimeContext().getMapState(new MapStateDescriptor<>("pending-click-json-v1", String.class, String.class));
        }
        private String enrich(JsonNode click, JsonNode imp) {
            ObjectNode copy = click.deepCopy();
            String reason = Measurement.exposureReason(imp, click, rules);
            copy.put("_link_reason", reason);
            if (reason.equals("OK")) copy.set("_impression", imp);
            return copy.toString();
        }
        @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode e = Json.read(value); long timestamp = e.path("event_time").asLong();
            if (timestamp + rules.retention <= ctx.timerService().currentWatermark()) {
                ctx.output(SIGNALS, signal(e, "LATE_REPLAY_REQUIRED", rules)); return;
            }
            if (Json.text(e, "event_type").equals("impression")) {
                if (impression.value() == null) {
                    impression.update(value); Measurement.impressionMetrics(e, rules, m -> ctx.output(METRICS, m));
                }
                JsonNode original = Json.read(impression.value());
                for (var pending : waiting.values()) out.collect(enrich(Json.read(pending), original));
                waiting.clear(); ctx.timerService().registerEventTimeTimer(timestamp + rules.retention);
            } else {
                if (impression.value() != null) out.collect(enrich(e, Json.read(impression.value())));
                else if (timestamp + rules.wait <= ctx.timerService().currentWatermark()) out.collect(enrich(e, null));
                else {
                    if (!waiting.contains(Json.business(e))) waiting.put(Json.business(e), value);
                    ctx.timerService().registerEventTimeTimer(timestamp + rules.wait);
                }
            }
        }
        @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            var remove = new ArrayList<String>();
            for (var entry : waiting.entries()) {
                var e = Json.read(entry.getValue());
                if (e.path("event_time").asLong() + rules.wait <= timestamp) {
                    out.collect(enrich(e, null)); remove.add(entry.getKey());
                }
            }
            for (String key : remove) waiting.remove(key);
            if (impression.value() != null && Json.read(impression.value()).path("event_time").asLong() + rules.retention <= timestamp)
                impression.clear();
        }
    }
    public static class ConversionJoin extends KeyedProcessFunction<String, String, String> {
        private final Rules rules;
        private transient ValueState<String> click;
        private transient MapState<String, String> waiting;
        private transient ValueState<Boolean> expired;
        private transient ValueState<Long> expiredUntil;
        public ConversionJoin(Rules rules) { this.rules = rules; }
        @Override public void open(OpenContext ctx) {
            click = getRuntimeContext().getState(new ValueStateDescriptor<>("click-json-v1", String.class));
            waiting = getRuntimeContext().getMapState(new MapStateDescriptor<>("pending-conversion-json-v1", String.class, String.class));
            expired = getRuntimeContext().getState(new ValueStateDescriptor<>("needs-replay-v1", Boolean.class));
            expiredUntil = getRuntimeContext().getState(new ValueStateDescriptor<>("needs-replay-until-v1", Long.class));
        }
        @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode e = Json.read(value); long timestamp = e.path("event_time").asLong();
            if (timestamp + rules.retention <= ctx.timerService().currentWatermark()) {
                ctx.output(SIGNALS, signal(e, "LATE_REPLAY_REQUIRED", rules)); return;
            }
            if (Json.text(e, "event_type").equals("click")) {
                if (click.value() == null) {
                    click.update(value); Measurement.clickMetrics(e, rules, m -> ctx.output(METRICS, m));
                    if (!Json.text(e, "_link_reason").equals("OK")) ctx.output(SIGNALS, signal(e, Json.text(e, "_link_reason"), rules));
                }
                JsonNode original = Json.read(click.value());
                for (String pending : waiting.values()) {
                    var conversion = Json.read(pending); String reason = Measurement.conversionReason(original, conversion, rules);
                    out.collect(Measurement.association(conversion, original, reason, rules).toString());
                    Measurement.conversionMetrics(conversion, original, reason, rules, m -> ctx.output(METRICS, m));
                }
                waiting.clear();
                if (Boolean.TRUE.equals(expired.value())) ctx.output(SIGNALS, signal(e, "LATE_CLICK_REPLAY_REQUIRED", rules));
                ctx.timerService().registerEventTimeTimer(timestamp + rules.retention);
            } else {
                if (click.value() != null || timestamp + rules.wait <= ctx.timerService().currentWatermark()) {
                    JsonNode c = click.value() == null ? null : Json.read(click.value());
                    String reason = Measurement.conversionReason(c, e, rules);
                    out.collect(Measurement.association(e, c, reason, rules).toString());
                    Measurement.conversionMetrics(e, c, reason, rules, m -> ctx.output(METRICS, m));
                    if (c == null) { expired.update(true); expiredUntil.update(timestamp + rules.retention); ctx.timerService().registerEventTimeTimer(timestamp + rules.retention); }
                } else {
                    if (!waiting.contains(Json.business(e))) {
                        waiting.put(Json.business(e), value);
                        out.collect(Measurement.association(e, null, "AWAITING_CLICK", rules).toString());
                    }
                    ctx.timerService().registerEventTimeTimer(timestamp + rules.wait);
                }
            }
        }
        @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            var remove = new ArrayList<String>();
            for (var entry : waiting.entries()) {
                var e = Json.read(entry.getValue());
                if (e.path("event_time").asLong() + rules.wait <= timestamp) {
                    out.collect(Measurement.association(e, null, "CLICK_NOT_FOUND", rules).toString());
                    Measurement.conversionMetrics(e, null, "CLICK_NOT_FOUND", rules, m -> ctx.output(METRICS, m));
                    ctx.output(SIGNALS, signal(e, "CLICK_NOT_FOUND", rules)); expired.update(true);
                    expiredUntil.update(e.path("event_time").asLong() + rules.retention);
                    ctx.timerService().registerEventTimeTimer(e.path("event_time").asLong() + rules.retention);
                    remove.add(entry.getKey());
                }
            }
            for (String key : remove) waiting.remove(key);
            if (click.value() != null && Json.read(click.value()).path("event_time").asLong() + rules.retention <= timestamp) {
                click.clear(); expired.clear();
            }
            if (expiredUntil.value() != null && timestamp >= expiredUntil.value()) { expired.clear(); expiredUntil.clear(); }
        }
    }
    public static class HistoricalDimension extends BroadcastProcessFunction<String, String, String> {
        private final Rules rules;
        public HistoricalDimension(Rules rules) { this.rules = rules; }
        @Override public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            ObjectNode m = (ObjectNode) Json.read(value); JsonNode best = null;
            long timestamp = m.path("event_time").asLong();
            for (var entry : ctx.getBroadcastState(DIMENSIONS).immutableEntries()) {
                JsonNode d = Json.read(entry.getValue());
                if (!Json.text(d, "campaign_id").equals(Json.text(m, "campaign_id"))) continue;
                if (d.path("effective_from").asLong() > timestamp) continue;
                if (!d.path("effective_to").isNull() && d.has("effective_to") && timestamp >= d.path("effective_to").asLong()) continue;
                if (best == null || d.path("source_version").asLong() > best.path("source_version").asLong()
                    || (d.path("source_version").asLong() == best.path("source_version").asLong() && d.path("effective_from").asLong() > best.path("effective_from").asLong())) best = d;
            }
            if (best != null) {
                JsonNode attributes = best.path("attributes");
                if (attributes.isTextual()) attributes = Json.read(attributes.asText());
                m.put("channel", best.path("deleted").asBoolean() ? "deleted" : attributes.path("channel").asText("unknown"));
            } else ctx.output(SIGNALS, signal(m, "DIMENSION_HISTORY_MISSING", rules));
            out.collect(Measurement.metricKey(m).toString());
        }
        @Override public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
            if (value == null || value.equals("null")) return;
            JsonNode envelope = Json.read(value); if (envelope.has("payload")) envelope = envelope.get("payload");
            if (envelope == null || envelope.isNull()) return;
            boolean deleted = envelope.path("op").asText().equals("d");
            JsonNode row = envelope.has("op") ? envelope.path(deleted ? "before" : "after") : envelope;
            if (!row.isObject() || !row.has("campaign_id")) return;
            ObjectNode dim = row.deepCopy(); dim.put("deleted", deleted || row.path("deleted").asBoolean());
            String key = Json.key(Json.text(dim, "campaign_id"), Json.text(dim, "effective_from"), Json.text(dim, "source_version"));
            ctx.getBroadcastState(DIMENSIONS).put(key, dim.toString());
            // Existing windows are repaired only through a separately validated release.
            ctx.output(SIGNALS, signal(dim, "DIMENSION_CHANGE_REPLAY_REQUIRED", rules));
        }
    }
    public static class Aggregate extends KeyedProcessFunction<String, String, String> {
        private final Rules rules;
        private transient MapState<String, String> contributions;
        private transient ValueState<String> aggregate;
        private transient ValueState<Long> nextEmit;
        private transient MapState<String, String> receipts;
        public Aggregate(Rules rules) { this.rules = rules; }
        @Override public void open(OpenContext ctx) {
            contributions = getRuntimeContext().getMapState(new MapStateDescriptor<>("contribution-values-json-v1", String.class, String.class));
            aggregate = getRuntimeContext().getState(new ValueStateDescriptor<>("full-metric-json-v1", String.class));
            nextEmit = getRuntimeContext().getState(new ValueStateDescriptor<>("next-emit-processing-v1", Long.class));
            receipts = getRuntimeContext().getMapState(new MapStateDescriptor<>("pending-visibility-json-v1", String.class, String.class));
        }
        @Override public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            ObjectNode input = (ObjectNode) Json.read(value); long cleanup = input.path("window_start").asLong() + rules.window + rules.retention;
            if (ctx.timerService().currentWatermark() >= cleanup) { ctx.output(SIGNALS, signal(input, "METRIC_FROZEN_REPLAY_REQUIRED", rules)); return; }
            String storedAggregate = aggregate.value();
            ObjectNode total = storedAggregate == null ? input.deepCopy() : (ObjectNode) Json.read(storedAggregate);
            ObjectNode values = storedAggregate == null ? Measurement.zeros() : (ObjectNode) total.path("values");
            String id = input.path("contribution_id").asText(); String previous = contributions.get(id);
            JsonNode priorValues = previous == null ? Measurement.zeros() : Json.read(previous);
            for (String field : Measurement.COUNTERS)
                values.put(field, values.path(field).asLong() - priorValues.path(field).asLong() + input.path("values").path(field).asLong());
            total.set("values", values); total.remove("contribution_id");
            total.put("received_at", nextEmit.value() == null ? input.path("received_at").asLong() : Math.min(total.path("received_at").asLong(), input.path("received_at").asLong()));
            String receipt = input.path("_receipt_id").asText();
            if (!receipt.isEmpty()) {
                ObjectNode visibility = Json.object(); visibility.put("receipt_id", receipt);
                visibility.put("received_at", input.path("received_at").asLong()); visibility.put("event_time", input.path("_source_event_time").asLong());
                receipts.put(receipt, visibility.toString());
            }
            total.remove("_receipt_id"); total.remove("_source_event_time");
            aggregate.update(total.toString()); contributions.put(id, input.path("values").toString());
            if (nextEmit.value() == null) {
                long next = ctx.timerService().currentProcessingTime() + rules.emit;
                nextEmit.update(next); ctx.timerService().registerProcessingTimeTimer(next);
            }
            ctx.timerService().registerEventTimeTimer(cleanup);
        }
        @Override public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (aggregate.value() == null) return;
            ObjectNode output = (ObjectNode) Json.read(aggregate.value());
            var visible = output.putArray("receipt_updates");
            for (String receipt : receipts.values()) visible.add(Json.read(receipt));
            out.collect(output.toString()); receipts.clear();
            if (ctx.timeDomain() == org.apache.flink.streaming.api.TimeDomain.EVENT_TIME) {
                contributions.clear(); aggregate.clear();
                if (nextEmit.value() != null) ctx.timerService().deleteProcessingTimeTimer(nextEmit.value());
            }
            nextEmit.clear();
        }
    }
    public static void main(String[] args) throws Exception {
        Rules rules = Rules.load(RuntimeConfig.env("CONTRACTS_DIR", "/opt/adpulse/contracts"), RuntimeConfig.env("RULES_FILE", "rules-v1.json"));
        var env = RuntimeConfig.environment("attribution-" + RuntimeConfig.release());
        var events = env.fromSource(RuntimeConfig.source(RuntimeConfig.topic("clean"), "adpulse-attribution-" + RuntimeConfig.release(), false),
            RuntimeConfig.watermarks(rules), "clean-kafka-v1").uid("clean-kafka-v1");
        var exposures = events.filter(s -> !Json.text(Json.read(s), "event_type").equals("conversion")).uid("exposure-click-route-v1")
            .keyBy(s -> Json.link(Json.read(s), "impression_id")).process(new ExposureJoin(rules)).name("exposure-association").uid("exposure-link-v1");
        var conversions = exposures.union(events.filter(s -> Json.text(Json.read(s), "event_type").equals("conversion")).uid("conversion-route-v1"))
            .keyBy(s -> Json.link(Json.read(s), "click_id")).process(new ConversionJoin(rules)).name("conversion-association").uid("conversion-link-v1");
        var dimensionStream = env.fromSource(RuntimeConfig.source(RuntimeConfig.env("DIMENSION_TOPIC", "adpulse.cdc.public.campaign_versions"), "adpulse-dimensions-" + RuntimeConfig.release(), false),
            WatermarkStrategy.<String>noWatermarks().withIdleness(java.time.Duration.ofMillis(rules.idle)), "dimension-cdc-v1").uid("dimension-cdc-v1");
        var enriched = exposures.getSideOutput(METRICS).union(conversions.getSideOutput(METRICS))
            .connect(dimensionStream.broadcast(DIMENSIONS)).process(new HistoricalDimension(rules)).name("historical-dimensions").uid("historical-dimensions-v1");
        var metrics = enriched.keyBy(s -> Json.read(s).path("metric_key").asText()).process(new Aggregate(rules)).name("full-metric-aggregate").uid("full-metric-aggregate-v1");
        metrics.union(conversions).sinkTo(RuntimeConfig.sink(RuntimeConfig.resultTopic(), "results", "output_key")).uid("results-kafka-sink-v1");
        exposures.getSideOutput(SIGNALS).union(conversions.getSideOutput(SIGNALS), enriched.getSideOutput(SIGNALS), metrics.getSideOutput(SIGNALS))
            .sinkTo(RuntimeConfig.sink(RuntimeConfig.topic("quality"), "attribution-quality", "receipt_id")).uid("attribution-quality-sink-v1");
        env.execute("AdPulse · attribution and metrics · " + rules.policyVersion);
    }
}
