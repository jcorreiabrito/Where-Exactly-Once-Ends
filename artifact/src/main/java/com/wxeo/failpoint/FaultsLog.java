package com.wxeo.failpoint;

import com.wxeo.util.IoUtil;
import com.wxeo.util.Json;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Append-only NDJSON fault log (docs/CONTRACTS.md §6, docs/method.md §5.6).
 *
 * <p>One line per action with exactly the frozen keys
 * {@code {ts_utc, run_id, fault, event, detail}}. Used by the in-process failpoints; S4's external
 * injectors append their own receipts to the same file (this class never rewrites it).</p>
 */
public final class FaultsLog {

    private final Path path;
    private final String runId;

    public FaultsLog(Path path, String runId) {
        this.path = path;
        this.runId = runId;
    }

    public synchronized void receipt(String fault, String event, String detail) {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("ts_utc", IoUtil.utcNowIso());
        line.put("run_id", runId);
        line.put("fault", fault);
        line.put("event", event);
        line.put("detail", detail);
        // Durable receipt: fsync before returning so an abrupt kill (crash action, process kill)
        // cannot lose the attestation (CONTRACTS §6, G4; S5 finding F-M2).
        IoUtil.appendLineFsync(path, Json.write(line));
    }

    public Path path() {
        return path;
    }
}
