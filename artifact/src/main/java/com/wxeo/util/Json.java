package com.wxeo.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;

/**
 * Central JSON helpers. Canonical serialization used by the artifact is deliberately hand-written
 * for the ledger event / entry / ground-truth line formats (see {@link com.wxeo.ledger.Ledger} and
 * {@link com.wxeo.ledger.GroundTruth}); Jackson is used for parsing and for rich objects
 * (configuration, metrics, manifests) where byte-exact canonical form is not part of the contract.
 */
public final class Json {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Json() {
    }

    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialize JSON", e);
        }
    }

    public static String writePretty(Object value) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("cannot serialize JSON", e);
        }
    }

    public static ObjectNode object() {
        return MAPPER.createObjectNode();
    }

    public static JsonNode parse(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("invalid JSON", e);
        }
    }

    public static <T> T read(String text, Class<T> type) {
        try {
            return MAPPER.readValue(text, type);
        } catch (IOException e) {
            throw new IllegalArgumentException("invalid JSON for " + type.getSimpleName(), e);
        }
    }

    public static ObjectNode toNode(Map<String, ?> map) {
        return MAPPER.valueToTree(map);
    }

    /** Renders a value object (map/pojo) as a one-line compact JSON object. */
    public static String line(Object value) {
        return write(value) + "\n";
    }
}
