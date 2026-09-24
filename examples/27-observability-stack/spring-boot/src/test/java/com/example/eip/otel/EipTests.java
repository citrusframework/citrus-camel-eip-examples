package com.example.eip.otel;

import com.example.eip.otel.config.EipInfraSetup;
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

@SpringBootTest(classes = ObservabilityApplication.class, webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    @Order(1)
    class TracedOrderPipelineTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldProcessOrderThroughTracedPipeline() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 149.99)
                    .variable("country", "US")
            );

            t.given(waitForCamelRouteStarted("traced-order-pipeline", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "OTEL-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-otel-sb-processed-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "customer_id": "CUST-00${id}",
                              "item_sku": "SKU-OT-${id}",
                              "quantity": 1,
                              "amount": ${amount},
                              "country": "${country}",
                              "valid": true,
                              "warehouse": "EAST-1",
                              "enriched_at": "@notEmpty()@"
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(2)
    class HealthProbeTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldReturnRouteStatusesFromHealthEndpoint() {
            t.given(waitForCamelRouteStarted("health-probe-handler", camelContext));

            t.when(
                http()
                    .client("http://localhost:8088")
                    .send()
                    .get("/api/health/routes")
            );

            // Verify 200 OK with application/json content type
            t.then(
                http()
                    .client("http://localhost:8088")
                    .receive()
                    .response(HttpStatus.OK)
                    .message()
                    .contentType("application/json")
            );
        }
    }

    @Nested
    @Order(3)
    class MetricsRestTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldReturnRouteStatusesFromMetricsEndpoint() {
            t.given(waitForCamelRouteStarted("metrics-orders-summary", camelContext));

            t.when(
                http()
                    .client("http://localhost:8088")
                    .send()
                    .get("/api/metrics/orders")
            );

            // Verify 200 OK with application/json content type
            t.then(
                http()
                    .client("http://localhost:8088")
                    .receive()
                    .response(HttpStatus.OK)
                    .message()
                    .contentType("application/json")
            );
        }
    }
}
