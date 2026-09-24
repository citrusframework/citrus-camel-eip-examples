package com.example.eip.drools;

import com.example.eip.drools.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = Application.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    @Order(1)
    class DroolsContentRouterTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldRouteStandardOrder() {
            // amount < 500, no hazmat, domestic -> standard
            t.given(waitForCamelRouteStarted("drools-content-router", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 2001, \"customer_id\": \"C-100\", \"amount\": 49.99, \"destination_country\": \"US\", \"contains_hazmat\": false}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-DROOLS-STD-2001")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.standard?consumerGroup=citrus-sb-drools-standard-group")
                            .message()
                            .body("""
                            {
                              "order_id": 2001,
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
                    .body("{\"order_id\": 2002, \"customer_id\": \"C-200\", \"amount\": 750.00, \"destination_country\": \"US\", \"contains_hazmat\": false}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-DROOLS-EXP-2002")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.express?consumerGroup=citrus-sb-drools-express-group")
                            .message()
                            .body("""
                            {
                              "order_id": 2002,
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
            // contains_hazmat = true -> hazmat (highest salience)
            t.given(waitForCamelRouteStarted("drools-content-router", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body("{\"order_id\": 2003, \"customer_id\": \"C-300\", \"amount\": 120.00, \"destination_country\": \"US\", \"contains_hazmat\": true}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-DROOLS-HAZ-2003")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.hazmat?consumerGroup=citrus-sb-drools-hazmat-group")
                            .message()
                            .body("""
                            {
                              "order_id": 2003,
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
                    .body("{\"order_id\": 2004, \"customer_id\": \"C-400\", \"amount\": 15000.00, \"destination_country\": \"DE\", \"contains_hazmat\": false}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SB-DROOLS-FRAUD-2004")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.fraud-review?consumerGroup=citrus-sb-drools-fraud-review-group")
                            .message()
                            .body("""
                            {
                              "order_id": 2004,
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
