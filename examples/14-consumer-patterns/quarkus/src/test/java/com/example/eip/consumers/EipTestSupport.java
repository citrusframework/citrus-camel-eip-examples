package com.example.eip.consumers;

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

    default TestActionBuilder<?> verifyCompletedExchanges(String routeId, long count, CamelContext camelContext) {
        return repeatOnError()
                .until((i, context) -> i > 20)
                .autoSleep(Duration.ofSeconds(1))
                .actions(
                    camel()
                        .camelContext(camelContext)
                        .route()
                        .verifyRouteStats(routeId)
                        .completed(count)
                );
    }

    default TestActionBuilder<?> verifyRouteStats(String routeId, String stats, CamelContext camelContext) {
        return repeatOnError()
                .until((i, context) -> i > 20)
                .autoSleep(Duration.ofSeconds(1))
                .actions(
                    camel()
                        .camelContext(camelContext)
                        .route()
                        .verifyRouteStats(routeId)
                        .stats(stats)
                );
    }
}
