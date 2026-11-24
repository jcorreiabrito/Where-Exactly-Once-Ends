package com.wxeo.util;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Child-process probe for {@link DurabilityTest}: performs a forced (fsynced) write and then halts
 * the JVM immediately, without any graceful close/flush, so the parent can prove the bytes reached
 * durable storage through the real fsync path.
 */
public final class DurabilityWriteProbe {

    public static void main(String[] args) throws Exception {
        byte[] bytes = Files.readAllBytes(Path.of(args[1]));
        IoUtil.writeFsync(Path.of(args[0]), bytes);
        Runtime.getRuntime().halt(0);
    }

    private DurabilityWriteProbe() {
    }
}
