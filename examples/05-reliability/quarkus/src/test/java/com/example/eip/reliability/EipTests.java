package com.example.eip.reliability;

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
    class DeadLetterChannelRouteTest {

        @Test
        public void shouldProcessOrderSuccessfully() {
            t.given(
                createVariables()
                    .variable("id", 1001)
                    .variable("amount", 100)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("dead-letter-channel-demo", camelContext));

            t.given(
                print().message("Send order ${id} (not divisible by 5) → should succeed")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .fork(true)
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                receive()
                    .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }

        @Test
        public void shouldRouteFailedOrderToDLQ() {
            t.given(
                createVariables()
                    .variable("id", 1000)
                    .variable("amount", 200)
                    .variable("status", "placed")
                    .variable("priority", "EXPRESS")
            );

            t.given(waitForCamelRouteStarted("dead-letter-channel-demo", camelContext));

            t.given(
                print().message("Send order ${id} (divisible by 5) → should fail and go to DLQ")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.dlq?consumerGroup=citrus-dlq-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    )
            );
        }
    }
}
