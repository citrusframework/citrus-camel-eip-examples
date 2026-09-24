package com.example.eip.channels;

import java.time.Duration;

import com.example.eip.channels.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.Processor;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.camel.dsl.CamelSupport;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.endpoint.KafkaMessageFilter;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spi.Resources;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;

import static org.citrusframework.kafka.endpoint.selector.KafkaMessageByHeaderSelector.kafkaHeaderEquals;

@SpringBootTest(classes = ChannelTypesApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Autowired
    StringRedisTemplate redisTemplate;

    @Nested
    class DatatypeChannelRouteTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldHandlePlacedOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("datatype-channel-router", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.placed")
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
                receive()
                    .endpoint("kafka:eip.orders.placed.typed?consumerGroup=citrus-placed-group-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }

        @Test
        public void shouldHandleCancelledOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
                    .variable("status", "cancelled")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("datatype-channel-router", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.placed")
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
                receive()
                    .endpoint("kafka:eip.orders.cancelled?consumerGroup=citrus-cancelled-group-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }

        @Test
        public void shouldHandleRefundedOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
                    .variable("status", "refunded")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("datatype-channel-router", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.placed")
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
                receive()
                    .endpoint("kafka:eip.orders.refunded?consumerGroup=citrus-refunded-group-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }

        @Test
        public void shouldHandleUnknownOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 200)
                    .variable("status", "unknown")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("datatype-channel-router", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.placed")
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
                receive()
                    .endpoint("kafka:eip.orders.unknown?consumerGroup=citrus-unknown-group-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }
    }

    @Nested
    class PointToPointRouteTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldHandlePlacedOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("point-to-point-consumer", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.placed")
            );

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
                    .times(5)
                    .actions(
                        receive()
                            .selector(KafkaMessageFilter.kafkaMessageFilter()
                                    .eventLookbackWindow(Duration.ofSeconds(10))
                                    .kafkaMessageSelector(kafkaHeaderEquals("order-id", "${id}"))
                                    .build())
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-group-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    )
            );
        }
    }

    @Nested
    class PublishSubscribeRouteTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldHandleProcessedOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("pubsub-publisher", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.processed")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.processed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("order-id", "${id}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(5)
                    .actions(
                        receive()
                            .selector(KafkaMessageFilter.kafkaMessageFilter()
                                    .eventLookbackWindow(Duration.ofSeconds(10))
                                    .kafkaMessageSelector(kafkaHeaderEquals("order-id", "${id}"))
                                    .build())
                            .endpoint("kafka:eip.orders.events?consumerGroup=citrus-events-group-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    )
            );
        }
    }

    @Nested
    class PulsarChannelRouteTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldHandlePlacedOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("point-to-point-pulsar", camelContext));

            t.given(
                print().message("Generate Pulsar order ${id} (status: ${status}) → eip.orders.placed")
            );

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                                .pulsar("persistent://public/default/eip.orders.placed")
                                .serviceUrl("pulsar://localhost:6650")
                                .producerName("citrus-demo-p2p")::getRawUri)
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            ).and(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .pulsar("persistent://public/default/eip.orders.events")
                            .serviceUrl("pulsar://localhost:6650")
                            .producerName("citrus-demo-pubsub")::getRawUri)
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }
    }

    @Nested
    class RedisChannelRouteTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldHandleOrderNotifications() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("status", "SHIPPED")
            );

            t.given(waitForCamelRouteStarted("redis-pubsub-subscriber", camelContext));

            t.given(
                print().message("Generate order ${id} (status: ${status}) → eip.orders.notifications")
            );

            t.when(
                camel()
                    .send()
                    .endpoint("log:info")
                    .message()
                    .body("""
                    {"order_id": ${id}, "customer_id": "CUST-00${id}", "event": "status_update", "status": "${status}"}
                    """)
                    .process(processor().camel()
                        .process()
                        .camelContext(camelContext)
                        .processor((Processor) exchange -> {
                            redisTemplate.convertAndSend("eip.orders.notifications",
                                    exchange.getMessage().getBody(String.class).strip());
                        }))
            );
        }
    }

}
