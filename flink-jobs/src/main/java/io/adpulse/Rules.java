package io.adpulse;

import com.fasterxml.jackson.databind.JsonNode;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;

public final class Rules implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String json;
    public final String ruleVersion, policyVersion;
    public final long attribution, wait, disorder, dedup, retention, window, emit, idle;
    public Rules(String json) {
        this.json = json;
        JsonNode n = Json.read(json);
        ruleVersion = n.path("rule_version").asText(); policyVersion = n.path("policy_version").asText();
        attribution = n.path("attribution_window_ms").asLong(); wait = n.path("join_wait_ms").asLong();
        disorder = n.path("out_of_order_ms").asLong(); dedup = n.path("dedup_ms").asLong();
        retention = n.path("state_retention_ms").asLong(); window = n.path("window_ms").asLong();
        emit = n.path("emit_interval_ms").asLong(); idle = n.path("idle_timeout_ms").asLong();
        if (attribution <= 0 || wait <= 0 || retention < attribution + wait || window <= 0 || emit <= 0 || idle <= 0 || dedup <= 0)
            throw new IllegalArgumentException("Invalid rule time boundaries");
    }
    public static Rules load(String directory, String file) throws Exception {
        String data = Files.readString(Path.of(directory, file));
        var schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(Files.readString(Path.of(directory, "rules.schema.json")));
        if (!schema.validate(Json.read(data)).isEmpty()) throw new IllegalArgumentException("Invalid rule schema");
        return new Rules(data);
    }
}
