package com.example.eip.drools;

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
    class DroolsContentRouterTest {

        @Test
        public void shouldRouteStandardOrder() {
            // amount < 500, no hazmat, domestic -> standard
            t.given(waitForCamelRouteStarted("drools-content-router", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1001, \"customer_id\": \"C-100\", \"amount\": 49.99, \"destination_country\": \"US\", \"contains_hazmat\": false}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "DROOLS-STD-1001")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.standard?consumerGroup=citrus-drools-standard-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1001,
                              "customer_id": "C-100",
                              "amount": 49.99,
                              "destination_country": "US",
                              "contains_hazmat": false
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldRouteExpressOrder() {
            // amount >= 500, domestic, no hazmat -> express
            t.given(waitForCamelRouteStarted("drools-content-router", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1002, \"customer_id\": \"C-200\", \"amount\": 750.00, \"destination_country\": \"US\", \"contains_hazmat\": false}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "DROOLS-EXP-1002")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.express?consumerGroup=citrus-drools-express-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1002,
                              "customer_id": "C-200",
                              "amount": 750.0,
                              "destination_country": "US",
                              "contains_hazmat": false
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldRouteHazmatOrder() {
            // contains_hazmat = true -> hazmat (highest salience, wins over express)
            t.given(waitForCamelRouteStarted("drools-content-router", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1003, \"customer_id\": \"C-300\", \"amount\": 120.00, \"destination_country\": \"US\", \"contains_hazmat\": true}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "DROOLS-HAZ-1003")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.hazmat?consumerGroup=citrus-drools-hazmat-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1003,
                              "customer_id": "C-300",
                              "amount": 120.0,
                              "destination_country": "US",
                              "contains_hazmat": true
                            }
                            """)
                    )
            );
        }

        @Test
        public void shouldRouteFraudReviewOrder() {
            // amount >= 10000, non-US destination, no hazmat -> fraud-review
            t.given(waitForCamelRouteStarted("drools-content-router", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 1004, \"customer_id\": \"C-400\", \"amount\": 15000.00, \"destination_country\": \"DE\", \"contains_hazmat\": false}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "DROOLS-FRAUD-1004")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.fraud-review?consumerGroup=citrus-drools-fraud-review-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1004,
                              "customer_id": "C-400",
                              "amount": 15000.0,
                              "destination_country": "DE",
                              "contains_hazmat": false
                            }
                            """)
                    )
            );
        }
    }
}
