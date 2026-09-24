package com.example.eip.otel;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.BindToRegistry;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.springframework.http.HttpStatus;

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
    class TracedOrderPipelineTest {

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
                    .header("kafka.KEY", "OTEL-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-otel-processed-group")
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

        @Test
        public void shouldReturnRouteStatusesFromHealthEndpoint() {
            t.given(waitForCamelRouteStarted("health-probe-handler", camelContext));

            t.when(
                http()
                    .client("http://localhost:8081")
                    .send()
                    .get("/health/routes")
            );

            // Verify 200 OK and that the response is a non-empty JSON array
            t.then(
                http()
                    .client("http://localhost:8081")
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

        @Test
        public void shouldReturnRouteStatusesFromMetricsEndpoint() {
            t.given(waitForCamelRouteStarted("metrics-orders-summary", camelContext));

            t.when(
                http()
                    .client("http://localhost:8081")
                    .send()
                    .get("/metrics/orders")
            );

            // Verify 200 OK and that the response is a non-empty JSON array
            t.then(
                http()
                    .client("http://localhost:8081")
                    .receive()
                    .response(HttpStatus.OK)
                    .message()
                    .contentType("application/json")
            );
        }
    }
}
