package com.example.eip.flags.config;

import java.util.Optional;

import dev.openfeature.contrib.providers.flagd.FlagdOptions;
import dev.openfeature.contrib.providers.flagd.FlagdProvider;
import dev.openfeature.sdk.Client;
import dev.openfeature.sdk.OpenFeatureAPI;
import org.citrusframework.TestAction;
import org.citrusframework.TestActionBuilder;
import org.citrusframework.annotations.CitrusConfiguration;
import org.citrusframework.api.container.AfterSuite;
import org.citrusframework.api.container.BeforeSuite;
import org.citrusframework.dsl.TestActionSupport;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.citrusframework.spi.BindToRegistry;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.citrusframework.container.SequenceAfterSuite.Builder.afterSuite;
import static org.citrusframework.container.SequenceBeforeSuite.Builder.beforeSuite;

@CitrusConfiguration
public class EipInfraSetup implements TestActionSupport {

    private static final Logger log = LoggerFactory.getLogger(EipInfraSetup.class);

    @BindToRegistry
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

    @BindToRegistry
    public AfterSuite stopInfra() {
        return afterSuite().actions(
                    camel().camelContext().stop(),
                    testcontainers().compose()
                            .down()
                            .containerName("eip-infra")
                ).build();
    }
}
