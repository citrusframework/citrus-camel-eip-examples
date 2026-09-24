package com.example.eip.flow;

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
    class SagaHappyPathTest {

        @Test
        public void shouldCompleteOrderSagaSuccessfully() {
            // order_id=1003: amount=149.99 (≤5000), 1003 % 7 == 2 (not 0) → SHIPPED → saga-completed
            t.given(waitForCamelRouteStarted("saga-orchestrator", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.saga")
                    .message()
                    .body("{\"order_id\": 1003, \"customer_id\": \"C-003\", \"amount\": 149.99, \"status\": \"placed\"}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SAGA-HAPPY-1003")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.saga-completed?consumerGroup=citrus-saga-completed-group")
                            .message()
                            .body("""
                            {
                              "order_id": 1003,
                              "customer_id": "C-003",
                              "amount": 149.99,
                              "status": "placed"
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(2)
    class SagaInventoryFailureTest {

        @Test
        public void shouldFailSagaWhenInventoryUnavailableForHighValueOrder() {
            // amount=9999.99 > 5000 → FAILED at inventory → saga-failed
            t.given(waitForCamelRouteStarted("saga-reserve-inventory", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.saga")
                    .message()
                    .body("{\"order_id\": 2001, \"customer_id\": \"C-002\", \"amount\": 9999.99, \"status\": \"placed\"}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SAGA-INV-FAIL-2001")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.saga-failed?consumerGroup=citrus-saga-inv-failed-group")
                            .message()
                            .body("""
                            {
                              "order_id": 2001,
                              "customer_id": "C-002",
                              "amount": 9999.99,
                              "status": "placed"
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(3)
    class SagaPaymentCompensationTest {

        @Test
        public void shouldCompensateInventoryWhenPaymentDeclined() {
            // order_id=7: amount=99.99 (≤5000), 7 % 7 == 0 → payment declined → compensate inventory → saga-failed
            t.given(waitForCamelRouteStarted("saga-authorize-payment", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.saga")
                    .message()
                    .body("{\"order_id\": 7, \"customer_id\": \"C-007\", \"amount\": 99.99, \"status\": \"placed\"}")
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "SAGA-PAY-COMP-7")
            );

            t.then(
                repeatOnError()
                    .times(20)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.saga-failed?consumerGroup=citrus-saga-pay-failed-group")
                            .message()
                            .body("""
                            {
                              "order_id": 7,
                              "customer_id": "C-007",
                              "amount": 99.99,
                              "status": "placed"
                            }
                            """)
                    )
            );
        }
    }
}
