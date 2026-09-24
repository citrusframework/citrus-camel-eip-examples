package com.example.eip.routing;

import java.time.Duration;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.BindToRegistry;
import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.citrusframework.actions.ReceiveTimeoutAction.Builder.expectTimeout;

@QuarkusTest
@CitrusSupport
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;

    @Inject
    @BindToRegistry
    CamelContext camelContext;

    @Nested
    class ContentBasedRouterTest {

        @Test
        public void shouldRouteDomesticOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 75.00)
                    .variable("country", "US")
                    .variable("hazmat", false)
            );

            t.given(waitForCamelRouteStarted("content-based-router", camelContext));

            t.given(
                print().message("Send US domestic order and verify routing to domestic topic")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                repeatOnError()
                    .times(25)
                    .autoSleep(Duration.ofMillis(500))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.domestic?consumerGroup=citrus-domestic-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }

        @Test
        public void shouldRouteInternationalOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 80.00)
                    .variable("country", "GB")
                    .variable("hazmat", false)
            );

            t.given(waitForCamelRouteStarted("content-based-router", camelContext));

            t.given(
                print().message("Send international GB order and verify routing to international topic")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                repeatOnError()
                    .times(25)
                    .autoSleep(Duration.ofMillis(500))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.international?consumerGroup=citrus-international-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }

        @Test
        public void shouldRouteHazmatOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 90.00)
                    .variable("country", "US")
                    .variable("hazmat", true)
            );

            t.given(waitForCamelRouteStarted("content-based-router", camelContext));

            t.given(
                print().message("Send hazmat order and verify routing to hazmat topic")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                repeatOnError()
                    .times(25)
                    .autoSleep(Duration.ofMillis(500))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.hazmat?consumerGroup=citrus-hazmat-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }

    @Nested
    class MessageFilterTest {

        @Test
        public void shouldPassHighValueOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 250.00)
                    .variable("country", "US")
                    .variable("hazmat", false)
            );

            t.given(waitForCamelRouteStarted("message-filter", camelContext));

            t.given(
                print().message("Send high-value order (amount >= 100) and verify it passes the filter")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                repeatOnError()
                    .times(25)
                    .autoSleep(Duration.ofMillis(500))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.high-value?consumerGroup=citrus-high-value-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }

        @Test
        public void shouldFilterLowValueOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 50.00)
                    .variable("country", "US")
                    .variable("hazmat", false)
            );

            t.given(waitForCamelRouteStarted("message-filter", camelContext));

            t.given(
                print().message("Send low-value order (amount < 100) and verify it is filtered out")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                expectTimeout()
                    .endpoint("kafka:eip.orders.high-value?consumerGroup=citrus-filter-reject-group")
                    .timeout(5000)
            );
        }
    }

    @Nested
    class RecipientListTest {

        @Test
        public void shouldNotifyAllRecipients() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 750.00)
            );

            t.given(waitForCamelRouteStarted("notification-recipient-list", camelContext));

            t.given(
                print().message("Send shipped order with phone and high amount to trigger all notifications")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.shipped")
                    .message()
                    .body(Resources.create("templates/shipped-order.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(sleep().seconds(5));

            t.then(
                camel().camelContext(camelContext)
                    .controlBus()
                    .route("notification-recipient-list")
                    .status()
                    .result(ServiceStatus.Started)
            );
        }
    }

    @Nested
    class SplitterTest {

        @Test
        public void shouldSplitBatchIntoIndividualItems() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
            );

            t.given(waitForCamelRouteStarted("order-splitter", camelContext));

            t.given(
                print().message("Send batch order with 2 items and verify both individual items on output topic")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.batch")
                    .message()
                    .body(Resources.create("templates/batch-order.json"))
                    .header("kafka.KEY", "BATCH-${id}")
            );

            t.then(
                repeatOnError()
                    .times(25)
                    .autoSleep(Duration.ofMillis(500))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.individual?consumerGroup=citrus-individual-group")
                            .message()
                            .body(Resources.create("templates/item.json")),
                        receive()
                            .endpoint("kafka:eip.orders.individual?consumerGroup=citrus-individual-group")
                            .message()
                            .body(Resources.create("templates/item.json"))
                    )
            );
        }
    }
}
