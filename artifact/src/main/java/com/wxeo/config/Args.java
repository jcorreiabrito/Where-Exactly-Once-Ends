package com.wxeo.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, dependency-free parser for the frozen CLI contracts (docs/CONTRACTS.md §7).
 *
 * <p>Accepts {@code --key=value} and {@code --key value}; all keys keep their exact spelling
 * including the {@code wxeo.} prefix. Also exposes {@link System#getProperty(String)} as a
 * secondary source so the appendix-B form {@code -Dwxeo.failpoints.enabled=true} works.</p>
 */
public final class Args {

    private final Map<String, String> values;
    private final List<String> raw;

    private Args(Map<String, String> values, List<String> raw) {
        this.values = values;
        this.raw = raw;
    }

    public static Args parse(String[] argv) {
        Map<String, String> map = new LinkedHashMap<>();
        List<String> raw = new ArrayList<>();
        for (int i = 0; i < argv.length; i++) {
            String a = argv[i];
            raw.add(a);
            if (!a.startsWith("--")) {
                continue;
            }
            String body = a.substring(2);
            int eq = body.indexOf('=');
            if (eq >= 0) {
                map.put(body.substring(0, eq), body.substring(eq + 1));
            } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                map.put(body, argv[i + 1]);
                raw.add(argv[++i]);
            } else {
                map.put(body, "true");
            }
        }
        return new Args(map, Collections.unmodifiableList(raw));
    }

    public static boolean hasHelpFlag(String[] argv) {
        for (String a : argv) {
            String body = a.startsWith("--") ? a.substring(2) : a;
            if (body.equals("wxeo.help") || body.equals("help") || body.equals("-h") || body.equals("--help")) {
                return true;
            }
        }
        return false;
    }

    /** Raw key-value map (immutable). */
    public Map<String, String> asMap() {
        return Collections.unmodifiableMap(values);
    }

    public List<String> raw() {
        return raw;
    }

    public boolean has(String key) {
        return values.containsKey(key);
    }

    public String get(String key, String def) {
        String v = values.get(key);
        if (v != null) {
            return v;
        }
        String sys = System.getProperty(key);
        return sys != null ? sys : def;
    }

    public String require(String key) {
        String v = get(key, null);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("missing required argument --" + key);
        }
        return v;
    }

    public int getInt(String key, int def) {
        String v = get(key, null);
        return v == null || v.isBlank() ? def : Integer.parseInt(v.trim());
    }

    public long getLong(String key, long def) {
        String v = get(key, null);
        return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
    }

    public boolean getBool(String key, boolean def) {
        String v = get(key, null);
        if (v == null || v.isBlank()) {
            return def;
        }
        return switch (v.trim().toLowerCase()) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> throw new IllegalArgumentException("invalid boolean for --" + key + ": " + v);
        };
    }

    /** All keys that start with the given prefix (sorted for deterministic output). */
    public Map<String, String> prefixed(String prefix) {
        Map<String, String> out = new java.util.TreeMap<>();
        values.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.put(k, v);
            }
        });
        return out;
    }
}
