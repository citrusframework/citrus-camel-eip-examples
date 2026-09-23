package com.example.eip.advanced;

import java.time.Duration;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.BindToRegistry;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.kafka.endpoint.KafkaMessageFilter;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.citrusframework.kafka.endpoint.selector.KafkaMessageByHeaderSelector.kafkaHeaderEquals;

@QuarkusTest
@CitrusSupport
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;

    @Inject
    @BindToRegistry
    CamelContext camelContext;

    @Nested
    class WireTapTest {

        @Test
        public void shouldProcessOrderAndSendAuditCopy() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", "150.00")
                    .variable("country", "US")
                    .variable("priority", "EXPRESS")
            );

            t.given(waitForCamelRouteStarted("wire-tap-main", camelContext));
            t.given(waitForCamelRouteStarted("wire-tap-audit", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.processing")
                    .message()
                    .fork(true)
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                parallel()
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-wiretap-processed-group")
                            .message()
                            .body(Resources.create("templates/processed-order.json")),
                        receive()
                            .endpoint("kafka:eip.orders.audit?consumerGroup=citrus-wiretap-audit-group")
                            .message()
                            .body(Resources.create("templates/audit-order.json"))
                    )
            );
        }
    }

    @Nested
    class DynamicRouterTest {

        @Test
        public void shouldRouteOrderThroughAllProcessingSteps() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", "200.00")
                    .variable("country", "US")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("dynamic-router-demo", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("order-id", "${id}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 10)
                    .autoSleep(Duration.ofSeconds(1))
                    .actions(
                        receive()
                            .selector(KafkaMessageFilter.kafkaMessageFilter()
                                    .eventLookbackWindow(Duration.ofSeconds(10))
                                    .kafkaMessageSelector(kafkaHeaderEquals("order-id", "${id}"))
                                    .build())
                            .endpoint("kafka:eip.orders.dynamic-routed?consumerGroup=citrus-dynamic-routed-group")
                            .message()
                            .body(Resources.create("templates/dynamic-routed-order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    )
            );
        }
    }

    @Nested
    class ResequencerTest {

        @Test
        public void shouldResequenceOutOfOrderMessages() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", "50.00")
                    .variable("country", "US")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("batch-resequencer", camelContext));

            t.given(
                print().message("Sending 3 messages with out-of-order sequence numbers: 3, 1, 2")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.sequenced")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("order-id", "${id}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    .header("sequenceNumber", 3)
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.sequenced")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("order-id", "${id}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    .header("sequenceNumber", 1)
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.sequenced")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("order-id", "${id}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    .header("sequenceNumber", 2)
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 20)
                    .autoSleep(Duration.ofSeconds(1))
                    .actions(
                        sequential()
                            .actions(
                                receive()
                                    .endpoint("kafka:eip.orders.resequenced?consumerGroup=citrus-resequenced-group")
                                    .message()
                                    .body(Resources.create("templates/order.json"))
                                    .header("sequenceNumber", 1),
                                receive()
                                    .endpoint("kafka:eip.orders.resequenced?consumerGroup=citrus-resequenced-group")
                                    .message()
                                    .body(Resources.create("templates/order.json"))
                                    .header("sequenceNumber", 2),
                                receive()
                                    .endpoint("kafka:eip.orders.resequenced?consumerGroup=citrus-resequenced-group")
                                    .message()
                                    .body(Resources.create("templates/order.json"))
                                    .header("sequenceNumber", 3)
                            )
                    )
            );
        }
    }

    @Nested
    class ComposedMessageProcessorTest {

        @Test
        public void shouldSplitProcessAndAggregateLineItems() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
            );

            t.given(waitForCamelRouteStarted("composed-message-processor", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.composed")
                    .message()
                    .body(Resources.create("templates/composed-order.json"))
                    .header("order-id", "${id}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 10)
                    .autoSleep(Duration.ofSeconds(1))
                    .actions(
                        receive()
                            .selector(KafkaMessageFilter.kafkaMessageFilter()
                                    .eventLookbackWindow(Duration.ofSeconds(10))
                                    .kafkaMessageSelector(kafkaHeaderEquals("order-id", "${id}"))
                                    .build())
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-group")
                            .message()
                            .body(Resources.create("templates/enriched-order.json"))
                    )
            );
        }
    }

    @Nested
    class LoadBalancerTest {

        @Test
        public void shouldDistributeOrdersAcrossFulfillmentCenters() {
            t.given(
                createVariables()
                    .variable("amount", "75.00")
                    .variable("country", "US")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("load-balancer-demo", camelContext));
            t.given(resetRouteStats(camelContext, "load-balancer-demo", "fulfillment-center-east", "fulfillment-center-central", "fulfillment-center-west"));

            for (int i = 0; i < 3; i++) {
                t.given(createVariables().variable("id", "citrus:randomNumber(4)"));
                t.when(
                    send()
                        .endpoint("kafka:eip.orders.loadbalanced")
                        .message()
                        .body(Resources.create("templates/order.json"))
                        .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                );
            }

            t.then(verifyCompletedExchanges("load-balancer-demo", 3, camelContext));
            t.then(verifyCompletedExchanges("fulfillment-center-east", 1, camelContext));
            t.then(verifyCompletedExchanges("fulfillment-center-central", 1, camelContext));
            t.then(verifyCompletedExchanges("fulfillment-center-west", 1, camelContext));
        }
    }
}
