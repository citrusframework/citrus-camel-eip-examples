package com.example.eip.management;

import com.example.eip.management.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.camel.dsl.CamelSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = EndpointManagementApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    @Order(1)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class SelectiveConsumerTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        @Order(1)
        public void shouldRejectHazmatOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 250)
            );

            t.given(waitForCamelRouteStarted("selective-consumer", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    .header("containsHazmat", "true")
            );

            t.then(
                expectTimeout()
                    .endpoint("kafka:eip.orders.accepted?consumerGroup=citrus-accepted-reject-group")
                    .timeout(5000)
            );
        }

        @Test
        @Order(2)
        public void shouldForwardNonHazmatOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 150)
            );

            t.given(waitForCamelRouteStarted("selective-consumer", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    .header("containsHazmat", "false")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.accepted?consumerGroup=citrus-accepted-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }

    @Nested
    @Order(2)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class ChannelPurgerTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        @Order(1)
        public void shouldPurgeStaleMessage() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 200)
            );

            t.given(waitForCamelRouteStarted("channel-purger", camelContext));

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .kafka("eip.orders.accepted")
                            .brokers("localhost:9092")::getRawUri)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
                    .header("orderTimestamp", String.valueOf(System.currentTimeMillis() - 3_600_000))
            );

            t.then(
                expectTimeout()
                    .endpoint("kafka:eip.orders.clean?consumerGroup=citrus-clean-group")
                    .timeout(5000)
            );
        }

        @Test
        @Order(2)
        public void shouldForwardRecentMessage() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
            );

            t.given(waitForCamelRouteStarted("channel-purger", camelContext));

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .kafka("eip.orders.accepted")
                            .brokers("localhost:9092")::getRawUri)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "${id}")
                    .header("orderTimestamp", String.valueOf(System.currentTimeMillis()))
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.clean?consumerGroup=citrus-clean-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }

    @Nested
    @Order(3)
    class MessagingGatewayTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldPublishOrderViaGateway() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 99)
            );

            t.given(waitForCamelRouteStarted("gateway-publish-order", camelContext));

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .direct("gateway-publish-order")::getRawUri)
                    .message()
                    .body(Resources.create("templates/order.json"))
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.placed?consumerGroup=citrus-gateway-placed-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }

        @Test
        public void shouldRequestInventoryViaGateway() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 75)
            );

            t.given(waitForCamelRouteStarted("gateway-request-inventory", camelContext));

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .direct("gateway-request-inventory")::getRawUri)
                    .message()
                    .body(Resources.create("templates/order.json"))
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.inventory-request?consumerGroup=citrus-gateway-inventory-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }

    @Nested
    @Order(4)
    class MessagingMapperTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldMapAndProcessOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 175)
            );

            t.given(waitForCamelRouteStarted("messaging-mapper", camelContext));
            t.given(resetRouteStats(camelContext, "messaging-mapper"));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.clean")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(verifyCompletedExchanges("messaging-mapper", 1, camelContext));
        }
    }
}
