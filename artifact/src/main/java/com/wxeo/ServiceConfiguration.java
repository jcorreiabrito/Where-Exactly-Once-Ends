package com.wxeo;

import com.wxeo.config.Args;
import com.wxeo.config.EffectiveConfig;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Spring wiring of the headless service: args → effective config → runner. */
@Configuration
public class ServiceConfiguration {

    @Bean
    public Args wxeoArgs(ApplicationArguments applicationArguments) {
        return Args.parse(applicationArguments.getSourceArgs());
    }

    @Bean
    public EffectiveConfig wxeoEffectiveConfig(Args args) {
        return EffectiveConfig.fromArgs(args);
    }

    @Bean(destroyMethod = "")
    public ServiceRunner wxeoRunner(EffectiveConfig config) {
        return new ServiceRunner(config);
    }
}
