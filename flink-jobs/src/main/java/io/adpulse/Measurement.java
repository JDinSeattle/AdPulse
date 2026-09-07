package io.adpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.function.Consumer;

/** Business definitions only. All Flink state is held by the operators. */
public final class Measurement {
    public static final String[] COUNTERS = {"impressions", "clicks", "matched_conversions", "unmatched_conversions", "converted_clicks", "value_minor"};
    public static final String[] DIMENSIONS = {"advertiser_id", "app_id", "campaign_id", "region", "app_version", "experiment_id", "variant", "currency", "channel"};
    private Measurement() {}
    public static ObjectNode zeros() {
        var values = Json.object(); for (String counter : COUNTERS) values.put(counter, 0L); return values;
    }
    public static String exposureReason(JsonNode impression, JsonNode click, Rules r) {
        if (impression == null) return "IMPRESSION_NOT_FOUND";
        if (!Json.sameIdentity(impression, click)) return "IMPRESSION_IDENTITY_MISMATCH";
        if (!Json.text(impression, "campaign_id").equals(Json.text(click, "campaign_id"))) return "IMPRESSION_CAMPAIGN_MISMATCH";
        long delta = click.path("event_time").asLong() - impression.path("event_time").asLong();
        if (delta < 0) return "CLICK_BEFORE_IMPRESSION";
        return delta > r.attribution ? "IMPRESSION_WINDOW_EXCEEDED" : "OK";
    }
    public static String conversionReason(JsonNode click, JsonNode conversion, Rules r) {
        if (click == null) return "CLICK_NOT_FOUND";
        if (!Json.sameIdentity(click, conversion)) return "IDENTITY_MISMATCH";
        if (!Json.text(click, "campaign_id").equals(Json.text(conversion, "campaign_id"))) return "CAMPAIGN_MISMATCH";
        long delta = conversion.path("event_time").asLong() - click.path("event_time").asLong();
        if (delta < 0) return "CLICK_AFTER_CONVERSION";
        return delta > r.attribution ? "ATTRIBUTION_WINDOW_EXCEEDED" : "MATCHED";
    }
    public static ObjectNode contribution(JsonNode e, JsonNode impression, long timestamp, String cohort,
                                            String currency, String count, long value, Rules r) {
        JsonNode dimension = impression == null ? e : impression;
        ObjectNode m = Json.object(); m.put("record_type", "metric");
        m.put("advertiser_id", Json.text(e, "advertiser_id")); m.put("app_id", Json.text(e, "app_id"));
        for (String field : new String[]{"campaign_id", "region", "app_version"}) m.put(field, Json.text(dimension, field));
        m.put("experiment_id", impression == null ? "unknown" : Json.text(impression, "experiment_id"));
        m.put("variant", impression == null ? "unknown" : Json.text(impression, "variant"));
        m.put("currency", currency); m.put("channel", "unknown");
        m.put("event_time", timestamp); m.put("window_start", timestamp / r.window * r.window); m.put("cohort_basis", cohort);
        m.put("rule_version", r.ruleVersion); m.put("policy_version", r.policyVersion);
        m.put("release_id", RuntimeConfig.release()); m.put("status", "provisional");
        m.put("received_at", e.path("received_at").asLong());
        m.put("_receipt_id", e.path("_receipt_id").asText());
        m.put("_source_event_time", e.path("event_time").asLong());
        String identity = count.equals("converted_clicks") ? Json.link(e, "click_id") : Json.business(e);
        m.put("contribution_id", Json.key(identity, cohort, currency, count));
        ObjectNode values = zeros(); values.put(count, value); m.set("values", values); return m;
    }
    public static ObjectNode metricKey(ObjectNode m) {
        var key = Json.MAPPER.createArrayNode();
        for (String field : DIMENSIONS) key.add(m.path(field).asText());
        key.add(m.path("window_start").asLong()); key.add(m.path("cohort_basis").asText()); key.add(m.path("policy_version").asText());
        m.put("metric_key", key.toString()); m.put("output_key", "m:" + key); return m;
    }
    public static ObjectNode association(JsonNode conversion, JsonNode click, String reason, Rules r) {
        var a = Json.object(); a.put("record_type", "association");
        a.put("association_key", Json.link(conversion, "conversion_id")); a.put("output_key", "a:" + Json.link(conversion, "conversion_id"));
        for (String field : new String[]{"advertiser_id", "app_id", "conversion_id", "click_id"}) a.put(field, Json.text(conversion, field));
        a.put("status", reason.equals("AWAITING_CLICK") ? "pending" : reason.equals("MATCHED") ? "matched" : "unmatched");
        a.put("reason", reason); a.put("policy_version", r.policyVersion); a.put("rule_version", r.ruleVersion);
        a.put("release_id", RuntimeConfig.release()); a.put("event_time", conversion.path("event_time").asLong());
        a.put("received_at", conversion.path("received_at").asLong()); a.put("receipt_id", conversion.path("_receipt_id").asText());
        JsonNode impression = click != null && reason.equals("MATCHED") ? click.get("_impression") : null;
        a.put("experiment_id", impression == null ? "unknown" : Json.text(impression, "experiment_id"));
        a.put("variant", impression == null ? "unknown" : Json.text(impression, "variant"));
        return a;
    }
    public static void impressionMetrics(JsonNode e, Rules r, Consumer<String> out) {
        for (String cohort : new String[]{"occurrence", "impression"})
            out.accept(contribution(e, e, e.path("event_time").asLong(), cohort, "ALL", "impressions", 1, r).toString());
    }
    public static void clickMetrics(JsonNode click, Rules r, Consumer<String> out) {
        JsonNode impression = click.get("_impression");
        for (String cohort : new String[]{"occurrence", "click"})
            out.accept(contribution(click, impression, click.path("event_time").asLong(), cohort, "ALL", "clicks", 1, r).toString());
        if (impression != null)
            out.accept(contribution(click, impression, impression.path("event_time").asLong(), "impression", "ALL", "clicks", 1, r).toString());
    }
    public static void conversionMetrics(JsonNode conversion, JsonNode click, String reason, Rules r, Consumer<String> out) {
        boolean matched = reason.equals("MATCHED");
        JsonNode impression = matched ? click.get("_impression") : null;
        out.accept(contribution(conversion, impression, conversion.path("event_time").asLong(), "occurrence", "ALL",
            matched ? "matched_conversions" : "unmatched_conversions", 1, r).toString());
        if (matched) {
            long timestamp = click.path("event_time").asLong();
            for (String counter : new String[]{"matched_conversions", "converted_clicks"})
                out.accept(contribution(conversion, impression, timestamp, "click", "ALL", counter, 1, r).toString());
            out.accept(contribution(conversion, impression, timestamp, "conversion_value", Json.text(conversion, "currency"), "matched_conversions", 1, r).toString());
            out.accept(contribution(conversion, impression, timestamp, "conversion_value", Json.text(conversion, "currency"), "value_minor", conversion.path("value_minor").asLong(), r).toString());
        }
    }
}
