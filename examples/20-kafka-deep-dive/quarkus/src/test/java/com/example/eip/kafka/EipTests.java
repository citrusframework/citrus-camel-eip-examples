package com.example.eip.kafka;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.BindToRegistry;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;

@QuarkusTest
@CitrusSupport
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;

    @Inject
    @BindToRegistry
    CamelContext camelContext;

    @Nested
    @Order(1)
    class TransactionalPipelineTest {

        @Test
        public void shouldEnrichHighPriorityOrder() {
            // order_id=1, amount=90.00 (> 60.0) -> priority=HIGH, warehouse=WH-2 (1 % 3 + 1 = 2)
            t.given(
                createVariables()
                    .variable("orderId", "1")
                    .variable("customerId", "C-001")
            );

            t.given(waitForCamelRouteStarted("transactional-pipeline", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1, \"customer_id\": \"C-001\", \"item_sku\": \"SKU-A1\", \"quantity\": 1, \"amount\": 90.00}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "TXN-HIGH-1")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-high-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1,
                              "customer_id": "C-001",
                              "amount": 90.00,
                              "status": "ENRICHED",
                              "warehouse": "WH-2",
                              "priority": "HIGH",
                              "enriched_at": "@ignore@"
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldEnrichStandardPriorityOrder() {
            // order_id=3, amount=29.99 (<= 60.0) -> priority=STANDARD, warehouse=WH-1 (3 % 3 + 1 = 1)
            t.given(waitForCamelRouteStarted("transactional-pipeline", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 3, \"customer_id\": \"C-003\", \"item_sku\": \"SKU-A1\", \"quantity\": 2, \"amount\": 29.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "TXN-STD-3")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-std-group")
                            .message()
                            .body("""
                            {
                              "order_id": 3,
                              "customer_id": "C-003",
                              "amount": 29.99,
                              "status": "ENRICHED",
                              "warehouse": "WH-1",
                              "priority": "STANDARD",
                              "enriched_at": "@ignore@"
                            }
                            """)
                    )
            );
        }
    }
}
