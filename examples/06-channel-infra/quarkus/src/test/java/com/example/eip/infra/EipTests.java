package com.example.eip.infra;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.BindToRegistry;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.camel.dsl.CamelSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

@QuarkusTest
@CitrusSupport
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;

    @Inject
    @BindToRegistry
    CamelContext camelContext;

    @Nested
    class ChannelAdapterTest {

        @Test
        public void shouldPersistAndPublishOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 99.95)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("channel-adapter-inbound", camelContext));

            t.given(
                print().message("Send order via inbound channel adapter (REST → PostgreSQL → Kafka)")
            );

            t.when(
                http()
                    .client("http://localhost:8081")
                    .send()
                    .post("/api/orders")
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .contentType("application/json")
            );

            t.then(
                receive()
                    .endpoint("kafka:eip.orders.incoming?consumerGroup=citrus-incoming-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
            );

            t.then(
                http()
                    .client("http://localhost:8081")
                    .receive()
                    .response(HttpStatus.OK)
                    .message()
                    .body("{\"status\": \"accepted\"}")
                    .contentType("application/json")
            );
        }
    }

    @Nested
    class MessageBusFulfillmentTest {

        @Test
        public void shouldFulfillBridgedOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 150)
                    .variable("status", "bridged")
                    .variable("priority", "EXPRESS")
            );

            t.given(waitForCamelRouteStarted("message-bus-fulfillment", camelContext));

            t.given(
                print().message("Send order to message bus bridged topic → should be fulfilled")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.bridged")
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
                            .endpoint("kafka:eip.orders.fulfilled?consumerGroup=citrus-fulfilled-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    )
            );
        }
    }

    @Nested
    class MessagingBridgePulsarToKafkaTest {

        @Test
        public void shouldBridgeOrderFromPulsarToKafka() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 200)
                    .variable("status", "partner-placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("messaging-bridge-pulsar-to-kafka", camelContext));

            t.given(
                print().message("Send partner order to Pulsar → should be bridged to Kafka")
            );

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .pulsar("persistent://public/default/partner.orders.placed")
                            .serviceUrl("pulsar://localhost:6650")
                            .producerName("citrus-test")::getRawUri)
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/order.json"))
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.placed?consumerGroup=citrus-placed-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }
}
