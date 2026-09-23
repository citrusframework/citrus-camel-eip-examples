package com.example.eip.management;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import org.apache.camel.api.management.ManagedCamelContext;
import org.apache.camel.api.management.mbean.ManagedRouteMBean;
import org.citrusframework.TestActionBuilder;
import org.citrusframework.dsl.TestActionSupport;
import org.citrusframework.exceptions.CitrusRuntimeException;

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

    default TestActionBuilder<?> resetRouteStats(CamelContext camelContext, String... routeIds) {
        return sequential()
                .actions(Arrays.stream(routeIds)
                        .map(routeId -> resetRouteStats(routeId, camelContext))
                        .collect(Collectors.toSet())
                        .toArray(TestActionBuilder[]::new));
    }

    default TestActionBuilder<?> resetRouteStats(String routeId, CamelContext camelContext) {
        return () -> (context) -> {
            ManagedCamelContext managedContext = camelContext.getCamelContextExtension()
                    .getContextPlugin(ManagedCamelContext.class);

            ManagedRouteMBean routeMBean = managedContext.getManagedRoute(routeId);

            if (routeMBean != null) {
                try {
                    routeMBean.reset(true);
                } catch (Exception e) {
                    throw new CitrusRuntimeException(String.format("Failed to reset route statistics for routeId '%s'", routeId), e);
                }
            } else {
                throw new CitrusRuntimeException(String.format("Failed to get managed route statistics for routeId '%s'", routeId));
            }
        };
    }
}
