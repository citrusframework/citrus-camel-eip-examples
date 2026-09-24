package com.example.eip.testing;

import com.example.eip.testing.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = TestingManagementApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    @Order(1)
    class DetourTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldEnrichOrderWhenEnrichmentEnabled() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 150)
            );

            t.given(waitForCamelRouteStarted("detour-enrichment", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "DETOUR-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "customer_id": "CUST-00${id}",
                              "item_sku": "SKU-TEST-${id}",
                              "quantity": 1,
                              "amount": ${amount},
                              "warehouse": "WH-EAST-01",
                              "estimated_weight_kg": 2.5,
                              "enriched": true
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(2)
    class SmartProxyTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldRoutePaymentToMockGateway() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 99)
            );

            t.given(waitForCamelRouteStarted("smart-proxy-router", camelContext));

            t.when(
                http()
                    .client("http://localhost:8082")
                    .send()
                    .post("/payments/process")
                    .message()
                    .body(Resources.create("templates/payment.json"))
                    .contentType("application/json")
            );

            t.then(
                http()
                    .client("http://localhost:8082")
                    .receive()
                    .response(HttpStatus.OK)
                    .message()
                    .body("""
                    {
                      "gateway": "MOCK",
                      "transaction_id": "@notEmpty()@",
                      "status": "APPROVED",
                      "message": "@notEmpty()@"
                    }
                    """)
                    .contentType("application/json")
            );
        }
    }

    @Nested
    @Order(3)
    class ManagedAdapterTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldProcessOrderAndCheckInventory() {
            t.given(
                createVariables()
                    .variable("id", "1")
                    .variable("amount", 200)
            );

            t.given(waitForCamelRouteStarted("managed-adapter-circuit-breaker", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.processed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ADAPTER-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.inventory-checked?consumerGroup=citrus-inventory-checked-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "customer_id": "CUST-001",
                              "item_sku": "SKU-TEST-1",
                              "quantity": 1,
                              "amount": ${amount},
                              "inventory_status": "IN_STOCK",
                              "warehouse_location": "WH-CENTRAL-02"
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldRouteToDeadLetterWhenCircuitBreaks() {
            // order_id=3 triggers the simulated failure (id % 3 == 0)
            t.given(
                createVariables()
                    .variable("id", "3")
                    .variable("amount", 300)
            );

            t.given(waitForCamelRouteStarted("managed-adapter-circuit-breaker", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.processed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ADAPTER-DLQ-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.dlq?consumerGroup=citrus-dlq-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "customer_id": "CUST-003",
                              "item_sku": "SKU-TEST-3",
                              "quantity": 1,
                              "amount": ${amount}
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(4)
    class TestMessagePatternTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldRouteRealOrderToProcessed() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 75)
            );

            t.given(waitForCamelRouteStarted("test-message-business-processor", camelContext));

            // Send a real (non-test) order — should be forwarded to eip.orders.processed
            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "REAL-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-tm-processed-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }
}
