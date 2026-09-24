package com.example.eip.channels;

import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import org.citrusframework.TestActionBuilder;
import org.citrusframework.dsl.TestActionSupport;

public interface EipTestSupport extends TestActionSupport {

    default TestActionBuilder<?> waitForCamelRouteStarted(String routeId, CamelContext camelContext) {
        return repeatOnError()
                .times(20)
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
