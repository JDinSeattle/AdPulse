package io.adpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OperatorsTest {
    @Test void cdcTombstoneIsSkippedButDeleteEnvelopeIsPreserved() {
        var values = new ArrayList<String>();
        var deserializer = new RuntimeConfig.ValueDeserializer();
        org.apache.flink.util.Collector<String> collector = new org.apache.flink.util.Collector<>() {
            public void collect(String value) { values.add(value); }
            public void close() {}
        };
        deserializer.deserialize(new org.apache.kafka.clients.consumer.ConsumerRecord<>("cdc", 0, 1, null, null), collector);
        assertTrue(values.isEmpty());
        deserializer.deserialize(new org.apache.kafka.clients.consumer.ConsumerRecord<>("cdc", 0, 2, null,
            "{\"op\":\"d\",\"before\":{\"campaign_id\":\"c\"}}".getBytes(java.nio.charset.StandardCharsets.UTF_8)), collector);
        assertEquals("d", Json.read(values.get(0)).path("op").asText());
    }
    private Rules rules() throws Exception { return Rules.load("../contracts", "rules-v1.json"); }
    @Test void frozenSchemaAllowlistIsEnforced() throws Exception {
        ObjectNode config = (ObjectNode) Json.read(rules().json); config.putArray("schema_versions").add(2);
        try (var h = new OneInputStreamOperatorTestHarness<String, String>(new ProcessOperator<>(new CleanJob.Validate(
            new Rules(config.toString()), Files.readString(Path.of("../contracts/event.schema.json")))))) {
            h.open(); JsonNode packet = fixture().path("packets").get(0);
            record(h, packet.toString()); assertTrue(drain(h).isEmpty());
            assertEquals("SCHEMA_VERSION_DISABLED", Json.read(side(h, CleanJob.QUALITY).get(0)).path("error_code").asText());
        }
    }
    private JsonNode fixture() throws Exception { return Json.read(Files.readString(Path.of("../tests/fixtures/oracle-baseline.json"))); }
    private ObjectNode conversion() throws Exception {
        for (JsonNode packet : fixture().path("packets")) if (packet.path("event").path("event_type").asText().equals("conversion")) {
            ObjectNode e = packet.path("event").deepCopy(); e.put("_receipt_id", packet.path("receipt_id").asText()); return e;
        }
        throw new AssertionError("No fixture conversion");
    }
    private ObjectNode clickFor(JsonNode conversion) throws Exception {
        for (JsonNode packet : fixture().path("packets")) {
            JsonNode e = packet.path("event");
            if (e.path("event_type").asText().equals("click") && e.path("click_id").asText().equals(conversion.path("click_id").asText())) {
                ObjectNode click = e.deepCopy(); click.put("_link_reason", "OK"); return click;
            }
        }
        throw new AssertionError("No fixture click");
    }
    private static List<String> drain(OneInputStreamOperatorTestHarness<String, String> h) {
        var values = new ArrayList<>(h.extractOutputValues()); h.getOutput().clear(); return values;
    }
    private static List<String> side(OneInputStreamOperatorTestHarness<String, String> h, OutputTag<String> tag) {
        var queue = h.getSideOutput(tag); var values = new ArrayList<String>();
        if (queue != null) { for (var record : queue) values.add(record.getValue()); queue.clear(); } return values;
    }
    private KeyedOneInputStreamOperatorTestHarness<String, String, String> conversionHarness(Rules rules) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AttributionJob.ConversionJoin(rules)),
            (KeySelector<String, String>) s -> Json.link(Json.read(s), "click_id"), Types.STRING);
    }
    private static void record(OneInputStreamOperatorTestHarness<String, String> h, String value) throws Exception {
        h.processElement(new StreamRecord<>(value, Json.read(value).path("event_time").asLong()));
    }
    @Test void completeStreamingOperatorsMatchIndependentPythonOracle() throws Exception {
        Rules r = rules(); JsonNode fixture = fixture();
        try (var validate = new OneInputStreamOperatorTestHarness<String, String>(new ProcessOperator<>(new CleanJob.Validate(r, Files.readString(Path.of("../contracts/event.schema.json")))));
             var id = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new CleanJob.Deduplicate(r, false)),
                (KeySelector<String, String>) s -> Json.link(Json.read(s), "event_id"), Types.STRING);
             var business = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new CleanJob.Deduplicate(r, true)),
                (KeySelector<String, String>) s -> Json.business(Json.read(s)), Types.STRING);
             var exposure = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AttributionJob.ExposureJoin(r)),
                (KeySelector<String, String>) s -> Json.link(Json.read(s), "impression_id"), Types.STRING);
             var conversion = conversionHarness(r);
             var aggregate = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AttributionJob.Aggregate(r)),
                (KeySelector<String, String>) s -> Json.read(s).path("metric_key").asText(), Types.STRING)) {
            for (var h : List.of(validate, id, business, exposure, conversion, aggregate)) h.open();
            Map<String, JsonNode> associations = new LinkedHashMap<>(); int dispositions = 0;
            for (JsonNode packet : fixture.path("packets")) {
                record(validate, packet.toString());
                for (String value : drain(validate)) record(id, value);
                for (String value : drain(id)) record(business, value);
                for (String value : drain(business)) {
                    if (Json.read(value).path("event_type").asText().equals("conversion")) record(conversion, value);
                    else record(exposure, value);
                    for (String click : drain(exposure)) record(conversion, click);
                    var contributions = side(exposure, AttributionJob.METRICS); contributions.addAll(side(conversion, AttributionJob.METRICS));
                    for (String contribution : contributions) record(aggregate, Measurement.metricKey((ObjectNode) Json.read(contribution)).toString());
                    for (String association : drain(conversion)) {
                        JsonNode a = Json.read(association); associations.put(a.path("association_key").asText(), a);
                    }
                }
                dispositions += side(validate, CleanJob.QUALITY).size() + side(id, CleanJob.QUALITY).size() + side(business, CleanJob.QUALITY).size();
            }
            aggregate.setProcessingTime(r.emit);
            Map<String, JsonNode> metrics = new LinkedHashMap<>();
            for (String value : drain(aggregate)) { var m = Json.read(value); metrics.put(m.path("metric_key").asText(), m.path("values")); }
            assertEquals(fixture.path("packets").size(), dispositions, "every accepted receipt must have exactly one cleaning disposition");
            assertEquals(fixture.path("expected").path("metrics").size(), metrics.size());
            for (JsonNode metric : fixture.path("expected").path("metrics"))
                assertEquals(metric.path("values"), metrics.get(metric.path("metric_key").asText()), metric.path("metric_key").asText());
            assertEquals(fixture.path("expected").path("associations").size(), associations.size());
            for (JsonNode expected : fixture.path("expected").path("associations")) {
                JsonNode actual = associations.get(expected.path("association_key").asText()); assertNotNull(actual);
                for (String field : new String[]{"status", "reason", "variant", "experiment_id"}) assertEquals(expected.path(field), actual.path(field));
            }
        }
    }
    @Test void conversionArrivesBeforeClickAndRestoresFromState() throws Exception {
        Rules r = rules(); ObjectNode conversion = conversion();
        OperatorSubtaskState snapshot;
        try (var h = conversionHarness(r)) {
            h.open(); record(h, conversion.toString());
            assertEquals("pending", Json.read(drain(h).get(0)).path("status").asText()); snapshot = h.snapshot(1, 1);
        }
        try (var recovered = conversionHarness(r)) {
            recovered.initializeState(snapshot); recovered.open(); record(recovered, clickFor(conversion).toString());
            assertEquals("matched", Json.read(drain(recovered).get(0)).path("status").asText());
        }
    }
    @Test void keyedStateCanRescaleFromOneToTwoWorkers() throws Exception {
        Rules r = rules(); ObjectNode conversion = conversion(); OperatorSubtaskState snapshot;
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AttributionJob.ConversionJoin(r)),
            (KeySelector<String, String>) s -> Json.link(Json.read(s), "click_id"), Types.STRING, 128, 1, 0)) {
            h.open(); record(h, conversion.toString()); snapshot = h.snapshot(2, 2);
        }
        String key = Json.link(conversion, "click_id"); int index = KeyGroupRangeAssignment.assignKeyToParallelOperator(key, 128, 2);
        OperatorSubtaskState repartitioned = AbstractHarnessRepartition.repartition(snapshot, index);
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AttributionJob.ConversionJoin(r)),
            (KeySelector<String, String>) s -> Json.link(Json.read(s), "click_id"), Types.STRING, 128, 2, index)) {
            h.initializeState(repartitioned); h.open(); record(h, clickFor(conversion).toString());
            assertEquals("matched", Json.read(drain(h).get(0)).path("status").asText());
        }
    }
    @Test void timeoutProducesReasonAndLateClickRequiresAuditedReplay() throws Exception {
        Rules r = rules(); ObjectNode conversion = conversion();
        try (var h = conversionHarness(r)) {
            h.open(); record(h, conversion.toString()); drain(h);
            h.processWatermark(new Watermark(conversion.path("event_time").asLong() + r.wait));
            assertEquals("CLICK_NOT_FOUND", Json.read(drain(h).get(0)).path("reason").asText());
            record(h, clickFor(conversion).toString());
            assertTrue(drain(h).isEmpty(), "past unmatched status remains in this release");
            assertTrue(side(h, AttributionJob.SIGNALS).stream().anyMatch(s -> s.contains("LATE_CLICK_REPLAY_REQUIRED")));
        }
    }
    @Test void actual24HourEdgesAndIdentityMismatch() throws Exception {
        Rules r = rules(); ObjectNode conversion = conversion(), click = clickFor(conversion);
        conversion.put("event_time", click.path("event_time").asLong() + r.attribution);
        assertEquals("MATCHED", Measurement.conversionReason(click, conversion, r));
        conversion.put("event_time", conversion.path("event_time").asLong() + 1);
        assertEquals("ATTRIBUTION_WINDOW_EXCEEDED", Measurement.conversionReason(click, conversion, r));
        conversion.put("event_time", click.path("event_time").asLong() - 1);
        assertEquals("CLICK_AFTER_CONVERSION", Measurement.conversionReason(click, conversion, r));
        conversion.put("user_id", "different"); assertEquals("IDENTITY_MISMATCH", Measurement.conversionReason(click, conversion, r));
    }
    @Test void dedupUsesProcessingTimeAndDoesNotConfuseWatermark() throws Exception {
        Rules r = rules(); ObjectNode event = conversion();
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new CleanJob.Deduplicate(r, true)),
            (KeySelector<String, String>) s -> Json.business(Json.read(s)), Types.STRING)) {
            h.open(); record(h, event.toString()); assertEquals(1, drain(h).size());
            h.processWatermark(new Watermark(Long.MAX_VALUE - 1)); record(h, event.toString());
            assertTrue(drain(h).isEmpty(), "event time must not expire processing-time dedup");
            h.setProcessingTime(r.dedup - 1); record(h, event.toString()); assertTrue(drain(h).isEmpty());
            h.setProcessingTime(r.dedup); record(h, event.toString()); assertEquals(1, drain(h).size());
        }
    }
    @Test void frozenMetricRoutesToCorrectionInsteadOfResettingAggregate() throws Exception {
        Rules r = rules(); ObjectNode event = conversion();
        String metric = Measurement.metricKey(Measurement.contribution(event, null, event.path("event_time").asLong(), "occurrence", "ALL", "impressions", 1, r)).toString();
        try (var h = new KeyedOneInputStreamOperatorTestHarness<>(new KeyedProcessOperator<>(new AttributionJob.Aggregate(r)),
            (KeySelector<String, String>) s -> Json.read(s).path("metric_key").asText(), Types.STRING)) {
            h.open(); record(h, metric); h.setProcessingTime(r.emit); assertEquals(1, drain(h).size());
            h.processWatermark(new Watermark(event.path("event_time").asLong() + r.retention + r.window)); drain(h);
            record(h, metric); h.setProcessingTime(r.emit * 2); assertTrue(drain(h).isEmpty());
            assertTrue(side(h, AttributionJob.SIGNALS).get(0).contains("METRIC_FROZEN_REPLAY_REQUIRED"));
        }
    }
    private static class AbstractHarnessRepartition {
        static OperatorSubtaskState repartition(OperatorSubtaskState state, int index) {
            return org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness.repartitionOperatorState(state, 128, 1, 2, index);
        }
    }
}
