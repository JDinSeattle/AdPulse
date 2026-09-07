package io.adpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public final class Json {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    private Json() {}
    public static JsonNode read(String value) {
        try { return MAPPER.readTree(value); } catch (Exception e) { throw new IllegalArgumentException("Invalid JSON", e); }
    }
    public static ObjectNode object() { return MAPPER.createObjectNode(); }
    public static String text(JsonNode node, String key) { return node.path(key).asText("unknown"); }
    public static String key(String... parts) {
        ArrayNode key = MAPPER.createArrayNode();
        for (String part : parts) key.add(part);
        return key.toString();
    }
    public static String business(JsonNode e) {
        String kind = text(e, "event_type");
        return key(text(e, "advertiser_id"), text(e, "app_id"), kind, text(e, kind + "_id"));
    }
    public static String link(JsonNode e, String field) {
        return key(text(e, "advertiser_id"), text(e, "app_id"), text(e, field));
    }
    public static boolean sameIdentity(JsonNode a, JsonNode b) {
        return text(a, "advertiser_id").equals(text(b, "advertiser_id"))
            && text(a, "app_id").equals(text(b, "app_id")) && text(a, "user_id").equals(text(b, "user_id"));
    }
}
