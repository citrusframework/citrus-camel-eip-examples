package com.example.eip.flags.config;

import dev.openfeature.contrib.providers.flagd.FlagdOptions;
import dev.openfeature.contrib.providers.flagd.FlagdProvider;
import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.OpenFeatureAPI;
import org.citrusframework.TestActionBuilder;
import org.citrusframework.api.container.AfterSuite;
import org.citrusframework.api.container.BeforeSuite;
import org.citrusframework.dsl.TestActionSupport;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.citrusframework.container.SequenceAfterSuite.Builder.afterSuite;
import static org.citrusframework.container.SequenceBeforeSuite.Builder.beforeSuite;

@Configuration
public class EipInfraSetup implements TestActionSupport {

    private static final Logger log = LoggerFactory.getLogger(EipInfraSetup.class);

    @Bean
    public BeforeSuite startInfra() {
        return beforeSuite().actions(
                    testcontainers().compose()
                            .up("_infra/compose.yaml")
                            .containerName("eip-infra")
                            .autoRemove(false),
                    waitFor()
                            .http()
                            .url("http://localhost:8090")
                            .seconds(25),
                    repeatOnError()
                            .times(25)
                            .actions(verifyFlagdConnectivity())
                ).build();
    }

    private TestActionBuilder<?> verifyFlagdConnectivity() {
        OpenFeatureAPI api = OpenFeatureAPI.getInstance();
        FlagdOptions options = FlagdOptions.builder()
                .host("localhost")
                .port(8013)
                .build();
        api.setProvider(new FlagdProvider(options));
        Client client = api.getClient("eip-shipping");

        return () -> context -> {
            if (client.getBooleanValue("enrichment-enabled", false)) {
                log.info("Connected to flagd eip-shipping!");
            } else {
                throw new CitrusRuntimeException("Not connected to flagd eip-shipping");
            }
        };
    }

    @Bean
    public AfterSuite stopInfra() {
        return afterSuite().actions(
                    camel().camelContext().stop(),
                    testcontainers().compose()
                            .down()
                            .containerName("eip-infra")
                ).build();
    }
}
