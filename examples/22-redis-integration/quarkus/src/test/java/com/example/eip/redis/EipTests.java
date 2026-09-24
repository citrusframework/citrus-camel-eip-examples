package com.example.eip.redis;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.BindToRegistry;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;

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
    class CachingEnricherTest {

        @Test
        public void shouldEnrichOrderWithCacheMiss() {
            // First request for customer C-101 — not yet cached, cache_hit=false
            t.given(waitForCamelRouteStarted("cached-enrichment", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1001, \"customer_id\": \"C-101\", \"item_sku\": \"SKU-R1\", \"quantity\": 2, \"amount\": 49.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORDER-1001")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-cache-miss-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1001,
                              "customer_id": "C-101",
                              "item_sku": "SKU-R1",
                              "quantity": 2,
                              "amount": 49.99,
                              "customer_name": "Customer C-101",
                              "cache_hit": false
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldEnrichOrderWithCacheHit() {
            // Second request for same customer C-101 — now cached, cache_hit=true
            t.given(waitForCamelRouteStarted("cached-enrichment", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1002, \"customer_id\": \"C-101\", \"item_sku\": \"SKU-R2\", \"quantity\": 1, \"amount\": 29.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORDER-1002")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-cache-hit-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1002,
                              "customer_id": "C-101",
                              "item_sku": "SKU-R2",
                              "quantity": 1,
                              "amount": 29.99,
                              "customer_name": "Customer C-101",
                              "cache_hit": true
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(2)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class IdempotentReceiverTest {

        @Test
        @Order(1)
        public void shouldProcessNewPaymentEvent() {
            t.given(waitForCamelRouteStarted("idempotent-receiver", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.payments")
                    .message()
                    .body("{\"event_id\": \"EVT-2001\", \"order_id\": 2001, \"amount\": 149.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "PAY-2001")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.payment-confirmed?consumerGroup=citrus-payment-confirmed-group")
                            .message()
                            .body("""
                            {
                              "event_id": "EVT-2001",
                              "order_id": 2001,
                              "amount": 149.99
                            }
                            """)
                    )
            );
        }

        @Test
        @Order(2)
        public void shouldDropDuplicatePaymentEvent() {
            // Re-send the same event_id — the idempotent receiver must deduplicate it.
            // The duplicate is routed to the handle-duplicate-payment named route.
            t.given(waitForCamelRouteStarted("idempotent-receiver", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.payments")
                    .message()
                    .body("{\"event_id\": \"EVT-2001\", \"order_id\": 2001, \"amount\": 149.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "PAY-2001-DUP")
            );

            t.then(verifyCompletedExchanges("handle-duplicate-payment", 1, camelContext));
        }
    }
}
