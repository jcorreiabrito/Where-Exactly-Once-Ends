package com.wxeo;

import com.wxeo.config.EffectiveConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.reflect.Modifier;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cheap Spring-context startup smoke test (no Kafka/PostgreSQL): catches the CGLIB/final-class
 * configuration class of bug that only surfaces on a live service start. Also asserts the service
 * entry point is not final (Spring Boot full-mode @Configuration classes are CGLIB-proxied).
 */
class ServiceContextStartupTest {

    @Test
    void configurationClassesAreProxyable() {
        assertFalse(Modifier.isFinal(ServiceConfiguration.class.getModifiers()),
                "@Configuration class must not be final (CGLIB full-mode proxy)");
        assertFalse(Modifier.isFinal(ServiceApplication.class.getModifiers()),
                "@SpringBootConfiguration class must not be final (CGLIB full-mode proxy)");
    }

    @Test
    void springContextRefreshesWithDryConfiguration(@TempDir Path dir) {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(ServiceApplication.class)
                .web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.OFF)
                .logStartupInfo(false)
                .run("--wxeo.mechanism=M0",
                        "--wxeo.run-id=context-startup-test",
                        "--wxeo.instance-id=1",
                        "--wxeo.bootstrap=127.0.0.1:9091",
                        "--wxeo.db-url=jdbc:postgresql://127.0.0.1:5433/wxeo",
                        "--wxeo.db-user=wxeo_app",
                        "--wxeo.output-dir=" + dir,
                        "--wxeo.faults-log=" + dir.resolve("faults.log"));
        try {
            assertNotNull(ctx.getBean(ServiceConfiguration.class));
            assertNotNull(ctx.getBean(EffectiveConfig.class));
            assertNotNull(ctx.getBean(ServiceRunner.class));
            EffectiveConfig cfg = ctx.getBean(EffectiveConfig.class);
            assertTrue(cfg.runId().equals("context-startup-test"));
            assertNotNull(cfg.sharedSha256());
        } finally {
            ctx.close();
        }
    }
}