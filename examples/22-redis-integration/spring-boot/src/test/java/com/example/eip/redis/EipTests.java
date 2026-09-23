package com.example.eip.redis;

import java.time.Duration;

import com.example.eip.redis.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spring.config.CitrusSpringConfig;
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

@SpringBootTest(classes = RedisIntegrationApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    @Order(1)
    class CachingEnricherTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldEnrichOrderWithCacheMiss() {
            // First request for customer C-201 — not yet cached, cache_hit=false
            t.given(waitForCamelRouteStarted("cached-enrichment", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1001, \"customer_id\": \"C-201\", \"item_sku\": \"SKU-R1\", \"quantity\": 2, \"amount\": 49.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-ORDER-1001")
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 20)
                    .autoSleep(Duration.ofSeconds(1))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-sb-enriched-cache-miss-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1001,
                              "customer_id": "C-201",
                              "item_sku": "SKU-R1",
                              "quantity": 2,
                              "amount": 49.99,
                              "customer_name": "Customer C-201",
                              "cache_hit": false
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldEnrichOrderWithCacheHit() {
            // Second request for same customer C-201 — now cached, cache_hit=true
            t.given(waitForCamelRouteStarted("cached-enrichment", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1002, \"customer_id\": \"C-201\", \"item_sku\": \"SKU-R2\", \"quantity\": 1, \"amount\": 29.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-ORDER-1002")
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 20)
                    .autoSleep(Duration.ofSeconds(1))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-sb-enriched-cache-hit-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1002,
                              "customer_id": "C-201",
                              "item_sku": "SKU-R2",
                              "quantity": 1,
                              "amount": 29.99,
                              "customer_name": "Customer C-201",
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

        @CitrusResource
        TestCaseRunner t;

        @Test
        @Order(1)
        public void shouldProcessNewPaymentEvent() {
            t.given(waitForCamelRouteStarted("idempotent-receiver", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.payments")
                    .message()
                    .body("{\"event_id\": \"EVT-SB-2001\", \"order_id\": 2001, \"amount\": 149.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-PAY-2001")
            );

            t.then(
                repeatOnError()
                    .until((i, context) -> i > 20)
                    .autoSleep(Duration.ofSeconds(1))
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.payment-confirmed?consumerGroup=citrus-sb-payment-confirmed-group")
                            .message()
                            .body("""
                            {
                              "event_id": "EVT-SB-2001",
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
                    .body("{\"event_id\": \"EVT-SB-2001\", \"order_id\": 2001, \"amount\": 149.99}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-PAY-2001-DUP")
            );

            t.then(verifyCompletedExchanges("handle-duplicate-payment", 1, camelContext));
        }
    }
}
