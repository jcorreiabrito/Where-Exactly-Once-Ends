package com.wxeo;

import com.wxeo.config.Args;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

/**
 * Headless Spring Boot entry point of the consumer service (one JVM per consumer instance).
 *
 * <p>Uses a Spring Boot application context without web server and without component scanning:
 * the two beans of {@link ServiceConfiguration} are wired explicitly, so the flat shaded jar also
 * runs the contract's {@code -cp ... com.wxeo.load.LoadGenerator} form from the same file.</p>
 *
 * <p>Usage (docs/CONTRACTS.md §7 — see {@code artifact/README.md} for every optional key):</p>
 * <pre>
 * java -jar artifact/target/wxeo-service.jar \
 *   --wxeo.mechanism=M0|M1-kafka|M1x|M2|M3|M4|M2+M3|M4+M3|M1x+M3 \
 *   --wxeo.ordering=strict|early --wxeo.run-id=... --wxeo.instance-id=1 \
 *   --wxeo.bootstrap=host:port --wxeo.db-url=jdbc:postgresql://host:port/db \
 *   --wxeo.db-user=... --wxeo.db-schema=wxeo_run_&lt;run_id&gt; \
 *   --wxeo.source-topic=... --wxeo.sink-topic=... --wxeo.group=... \
 *   --wxeo.output-dir=&lt;scratch&gt; --wxeo.faults-log=&lt;path&gt; \
 *   [--wxeo.failpoint.enabled=true --wxeo.failpoint.name=&lt;name&gt; \
 *    --wxeo.failpoint.action=crash|throw|abort_txn|pause[:ms] --wxeo.failpoint.after=N]
 * </pre>
 */
@SpringBootConfiguration
@Import(ServiceConfiguration.class)
public final class ServiceApplication {

    public static void main(String[] args) {
        if (Args.hasHelpFlag(args) || args.length == 0) {
            System.out.println(usage());
            System.exit(args.length == 0 ? 2 : 0);
            return;
        }
        ConfigurableApplicationContext ctx;
        try {
            ctx = new SpringApplicationBuilder(ServiceApplication.class)
                    .web(WebApplicationType.NONE)
                    .bannerMode(org.springframework.boot.Banner.Mode.OFF)
                    .logStartupInfo(false)
                    .run(args);
        } catch (Throwable t) {
            System.err.println("[wxeo] cannot start Spring context: " + t);
            t.printStackTrace(System.err);
            System.exit(2);
            return;
        }

        ServiceRunner runner = ctx.getBean(ServiceRunner.class);
        Thread hook = new Thread(() -> {
            try {
                runner.requestStop("signal");
                runner.awaitTermination(60_000);
                runner.writeApplicationArtifacts();
            } finally {
                try {
                    ctx.close();
                } catch (RuntimeException ignored) {
                    // context may already be closing
                }
            }
        }, "wxeo-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(hook);

        int exit = 0;
        try {
            runner.runBlocking();
            if (runner.fatalError()) {
                exit = 1;
            }
        } catch (Throwable t) {
            System.err.println("[wxeo] fatal: " + t);
            t.printStackTrace(System.err);
            exit = 1;
        } finally {
            try {
                runner.writeApplicationArtifacts();
            } catch (Throwable ignored) {
                // best effort; artifacts already attempted in shutdown()
            }
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down
            }
            try {
                ctx.close();
            } catch (RuntimeException ignored) {
                // ignore
            }
        }
        System.exit(exit);
    }

    public static String usage() {
        return String.join("\n",
                "wxeo-service — reference implementation for \"Where Exactly-Once Ends\"",
                "",
                "REQUIRED",
                "  --wxeo.mechanism=M0|M1-kafka|M1x|M2|M3|M4|M2+M3|M4+M3|M1x+M3",
                "  --wxeo.run-id=<run_id>          --wxeo.instance-id=<n>",
                "  --wxeo.bootstrap=host:port      --wxeo.source-topic=<topic>",
                "  --wxeo.sink-topic=<topic>       --wxeo.group=<consumer-group>",
                "  --wxeo.output-dir=<dir>         --wxeo.faults-log=<path>",
                "  --wxeo.db-url=jdbc:postgresql://host:port/db  (all except M1-kafka)",
                "  --wxeo.db-user=<user>           --wxeo.db-schema=wxeo_run_<run_id>",
                "",
                "OPTIONAL",
                "  --wxeo.ordering=strict|early    (default strict; M1-kafka n/a)",
                "  --wxeo.producer-bootstrap=host:port  (optional; F3 semantic targeting)",
                "  --wxeo.consumer-bootstrap=host:port  (optional; F4a semantic targeting)",
                "  --wxeo.schema-init=none|auto    (default none)",
                "  --wxeo.require-append-only-attestation=true|false (default false)",
                "  --wxeo.failpoint.enabled=true|false  (or -Dwxeo.failpoints.enabled=true)",
                "  --wxeo.failpoint.name=after_fetch|before_effect_commit|after_effect_commit|",
                "                        before_progress_commit|after_progress_commit|before_publish|",
                "                        before_relay_mark",
                "  --wxeo.failpoint.action=crash|throw|abort_txn|pause[:ms]|none",
                "  --wxeo.failpoint.after=<N>      (fire on the N-th hit; default 1)",
                "  --wxeo.failpoint.pause-ms=<ms>",
                "",
                "GRACEFUL STOP: create <output-dir>/stop_<run_id>_<instance>.flag (or send SIGTERM/SIGINT).",
                "ARTIFACTS: config_/app_/hist_/ready_<run_id>_<instance>.* in --wxeo.output-dir.");
    }

    private ServiceApplication() {
    }
}
