package com.wxeo.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;

/** Filesystem helpers: durable writes (write + fsync), SHA-256, UTC timestamps. */
public final class IoUtil {

    private IoUtil() {
    }

    public static Path ensureDir(Path dir) {
        try {
            Files.createDirectories(dir);
            return dir;
        } catch (IOException e) {
            throw new IllegalStateException("cannot create directory " + dir, e);
        }
    }

    /**
     * Writes bytes and forces them to disk before returning (durability for ground truth).
     *
     * <p>Uses an explicit {@link java.nio.channels.FileChannel} and {@link java.nio.channels.FileChannel#force(boolean)}
     * because {@code Files.newOutputStream} does <b>not</b> return a {@code FileOutputStream} on the
     * default providers (it returns a channel-backed stream), so an {@code instanceof FileOutputStream}
     * check would be a silent no-op — the exact defect S5 found. On Windows {@code force(true)} maps to
     * {@code FlushFileBuffers}.</p>
     */
    public static void writeFsync(Path path, byte[] bytes) {
        ensureDir(path.toAbsolutePath().getParent());
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("cannot write " + path, e);
        }
    }

    public static void writeStringFsync(Path path, String text) {
        writeFsync(path, text.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Atomically installs {@code bytes} at {@code path}: write to a sibling temp file, fsync it,
     * then move it into place. Used for the config file so a partially written file is never
     * observable.
     */
    public static void writeAtomicFsync(Path path, String text) {
        ensureDir(path.toAbsolutePath().getParent());
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        writeStringFsync(tmp, text);
        try {
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            try {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e2) {
                throw new IllegalStateException("cannot move " + tmp + " -> " + path, e2);
            }
        }
    }

    /** Appends one line and flushes it (used for buffered, non-durable NDJSON logs). */
    public static void appendLine(Path path, String line) {
        ensureDir(path.toAbsolutePath().getParent());
        byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        try (OutputStream out = Files.newOutputStream(path,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)) {
            out.write(bytes);
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException("cannot append to " + path, e);
        }
    }

    /**
     * Appends one line and fsyncs it before returning (fault receipts must survive an abrupt
     * process kill; docs/CONTRACTS.md §6, S5 finding F-M2). Opens the file with
     * {@code FileOutputStream(append=true)} so {@link java.io.FileDescriptor#sync()} is real.
     */
    public static void appendLineFsync(Path path, String line) {
        ensureDir(path.toAbsolutePath().getParent());
        byte[] bytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(path.toFile(), true)) {
            fos.write(bytes);
            fos.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new IllegalStateException("cannot fsync-append to " + path, e);
        }
    }

    /** Creates {@code path} atomically only if absent; returns true when the caller created it. */
    public static boolean createExclusive(Path path) throws IOException {
        try (var channel = java.nio.channels.FileChannel.open(path,
                java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE)) {
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        }
    }

    public static String sha256Hex(byte[] bytes) {
        return HexFormat.of().formatHex(sha256().digest(bytes));
    }

    public static String sha256Hex(Path path) {
        MessageDigest md = sha256();
        try (var in = Files.newInputStream(path)) {
            byte[] buf = new byte[1 << 16];
            int r;
            while ((r = in.read(buf)) > 0) {
                md.update(buf, 0, r);
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot hash " + path, e);
        }
        return HexFormat.of().formatHex(md.digest());
    }

    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static String utcNowIso() {
        return utcIso(System.currentTimeMillis());
    }

    public static String utcIso(long epochMs) {
        return Instant.ofEpochMilli(epochMs).truncatedTo(ChronoUnit.MILLIS).toString();
    }

    public static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
