package com.example.eip.composed;

import java.time.Duration;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.BindToRegistry;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@CitrusSupport
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;

    @Inject
    @BindToRegistry
    CamelContext camelContext;

    @Nested
    class RoutingSlipDomesticTest {

        @Test
        public void shouldRouteStandardDomesticOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("country", "US")
                    .variable("hazmat", false)
            );

            t.given(waitForCamelRouteStarted("order-routing-slip", camelContext));
            t.given(resetRouteStats(camelContext, "order-routing-slip", "validate-order", "assign-carrier", "hazmat-compliance", "customs-classification"));

            t.given(
                print().message("Send standard US domestic order — slip: validate → assign-carrier")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(verifyCompletedExchanges("order-routing-slip", 1, camelContext));
            t.then(verifyCompletedExchanges("validate-order", 1, camelContext));
            t.then(verifyCompletedExchanges("assign-carrier", 1, camelContext));
        }
    }

    @Nested
    class RoutingSlipHazmatTest {

        @Test
        public void shouldRouteHazmatOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("country", "US")
                    .variable("hazmat", true)
            );

            t.given(waitForCamelRouteStarted("order-routing-slip", camelContext));
            t.given(resetRouteStats(camelContext, "order-routing-slip", "validate-order", "assign-carrier", "hazmat-compliance", "customs-classification"));

            t.given(
                print().message("Send hazmat US order — slip: validate → hazmat-compliance → assign-carrier")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(verifyCompletedExchanges("validate-order", 1, camelContext));
            t.then(verifyCompletedExchanges("hazmat-compliance", 1, camelContext));
            t.then(verifyCompletedExchanges("assign-carrier", 1, camelContext));
        }
    }

    @Nested
    class RoutingSlipInternationalTest {

        @Test
        public void shouldRouteInternationalOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("country", "DE")
                    .variable("hazmat", false)
            );

            t.given(waitForCamelRouteStarted("order-routing-slip", camelContext));
            t.given(resetRouteStats(camelContext, "order-routing-slip", "validate-order", "assign-carrier", "hazmat-compliance", "customs-classification"));

            t.given(
                print().message("Send international DE order — slip: validate → customs-classification → assign-carrier")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(verifyCompletedExchanges("validate-order", 1, camelContext));
            t.then(verifyCompletedExchanges("customs-classification", 1, camelContext));
            t.then(verifyCompletedExchanges("assign-carrier", 1, camelContext));
        }
    }

    @Nested
    class RoutingSlipFullPipelineTest {

        @Test
        public void shouldRouteInternationalHazmatOrderThroughAllSteps() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("country", "DE")
                    .variable("hazmat", true)
            );

            t.given(waitForCamelRouteStarted("order-routing-slip", camelContext));
            t.given(resetRouteStats(camelContext, "order-routing-slip", "validate-order", "assign-carrier", "hazmat-compliance", "customs-classification"));

            t.given(
                print().message("Send international hazmat order — slip: validate → hazmat-compliance → customs-classification → assign-carrier")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(verifyCompletedExchanges("order-routing-slip", 1, camelContext));
            t.then(verifyCompletedExchanges("validate-order", 1, camelContext));
            t.then(verifyCompletedExchanges("hazmat-compliance", 1, camelContext));
            t.then(verifyCompletedExchanges("customs-classification", 1, camelContext));
            t.then(verifyCompletedExchanges("assign-carrier", 1, camelContext));
        }
    }

    @Nested
    class ScatterGatherTest {

        @Test
        public void shouldSelectBestCarrierRate() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("weight", 5.0)
                    .variable("country", "US")
            );

            t.given(waitForCamelRouteStarted("carrier-scatter-gather", camelContext));
            t.given(resetRouteStats(camelContext, "order-routing-slip", "validate-order", "assign-carrier", "hazmat-compliance", "customs-classification"));

            t.given(
                print().message("Send rate request and verify best carrier rate on output topic")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.shipping.rate-requests")
                    .message()
                    .body(Resources.create("templates/rate-request.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 25)
                    .autoSleep(Duration.ofMillis(500))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.shipping.best-rate?consumerGroup=citrus-best-rate-group")
                            .message()
                    )
            );
        }
    }
}
