package com.example.eip.endpoints;

import java.time.Duration;
import javax.sql.DataSource;

import com.example.eip.endpoints.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.camel.dsl.CamelSupport;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spi.Resources;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

import static org.citrusframework.actions.CreateVariablesAction.Builder.createVariables;
import static org.citrusframework.actions.ReceiveMessageAction.Builder.receive;
import static org.citrusframework.actions.SendMessageAction.Builder.send;

@SpringBootTest(classes = EndpointsApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Autowired
    DataSource dataSource;

    @Nested
    class DurableSubscriberTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldConsumeFromPulsarDurably() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 175)
            );

            t.given(waitForCamelRouteStarted("durable-subscriber-pulsar", camelContext));

            t.when(
                camel()
                    .send()
                    .endpoint(CamelSupport.camel().endpoints()
                            .pulsar("persistent://public/default/eip.orders.placed")
                            .serviceUrl("pulsar://localhost:6650")
                            .producerName("citrus-durable-sub-test")::getRawUri)
                    .message()
                    .body(Resources.create("templates/order.json"))
            );

            t.then(verifyCompletedExchanges("durable-subscriber-pulsar", 1, camelContext));
        }
    }

    @Nested
    class IdempotentReceiverTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldDeduplicateIdenticalOrders() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 100)
            );

            t.given(waitForCamelRouteStarted("idempotent-receiver", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.deduplicated?consumerGroup=citrus-dedup-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );

            t.then(
                expectTimeout()
                    .endpoint("kafka:eip.orders.deduplicated?consumerGroup=citrus-dedup-group")
                    .timeout(5000)
            );

            t.then(verifyCompletedExchanges("idempotent-receiver", 2, camelContext));
        }

        @Test
        public void shouldPassThroughUniqueOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 200)
            );

            t.given(waitForCamelRouteStarted("idempotent-receiver", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.deduplicated?consumerGroup=citrus-unique-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }

    @Nested
    class ServiceActivatorTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldEnrichOrderWithInventoryData() {
            t.given(
                createVariables()
                    .variable("id", 1001)
                    .variable("amount", 300)
            );

            t.given(waitForCamelRouteStarted("service-activator", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.deduplicated")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.inventory-checked?consumerGroup=citrus-inventory-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "customer_id": "CUST-00${id}",
                              "item_sku": "SKU-SHIP-${id}",
                              "quantity": 1,
                              "amount": ${amount},
                              "in_stock": true,
                              "available_quantity": 50,
                              "inventory_checked_at": "@ignore@"
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    class TransactionalOutboxTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldProcessPaymentAndPublishViaOutbox() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 500)
            );

            t.given(waitForCamelRouteStarted("transactional-client", camelContext));
            t.given(waitForCamelRouteStarted("outbox-publisher", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.payments.required")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        sql(dataSource)
                            .query()
                            .statement("SELECT order_id, status FROM payments.payments WHERE order_id = '${id}'")
                            .validate("order_id", "${id}")
                            .validate("status", "PROCESSED")
                    )
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .autoSleep(Duration.ofSeconds(2))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.payments.processed?consumerGroup=citrus-payments-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "amount": ${amount},
                              "status": "PROCESSED"
                            }
                            """)
                    )
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        sql(dataSource)
                            .query()
                            .statement("SELECT published FROM payments.outbox WHERE aggregate_id = '${id}'")
                            .validate("published", "true")
                    )
            );
        }
    }
}
