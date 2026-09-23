package com.example.eip.testing;

import java.time.Duration;

import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import org.citrusframework.TestActionBuilder;
import org.citrusframework.dsl.TestActionSupport;

public interface EipTestSupport extends TestActionSupport {

    default TestActionBuilder<?> waitForCamelRouteStarted(String routeId, CamelContext camelContext) {
        return repeatOnError()
                .until((i, context) -> i > 20)
                .autoSleep(Duration.ofSeconds(1))
                .actions(
                    camel().camelContext(camelContext)
                            .controlBus()
                            .route(routeId)
                            .status()
                            .result(ServiceStatus.Started)
                            .description("Waiting for Camel route '%s' to be started ...".formatted(routeId)),
                    sleep().seconds(5)
                );
    }
}
