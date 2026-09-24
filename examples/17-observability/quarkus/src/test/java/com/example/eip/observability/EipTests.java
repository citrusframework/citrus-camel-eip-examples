package com.example.eip.observability;

import javax.sql.DataSource;

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

@QuarkusTest
@CitrusSupport
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
class EipTests implements EipTestSupport {

    @CitrusResource
    TestCaseRunner t;

    @Inject
    @BindToRegistry
    CamelContext camelContext;

    @Inject
    @BindToRegistry
    DataSource dataSource;

    @Nested
    @Order(1)
    class WireTapTest {

        @Test
        public void shouldProcessOrderToOutput() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 75)
            );

            t.given(waitForCamelRouteStarted("wiretap-order-processor", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "WT-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-wt-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }

        @Test
        public void shouldWireTapOrderToAudit() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 85)
            );

            t.given(waitForCamelRouteStarted("audit-log-writer", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "AUDIT-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.audit?consumerGroup=citrus-audit-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                    )
            );
        }
    }

    @Nested
    @Order(2)
    class MessageHistoryTest {

        @Test
        public void shouldEnrichOrderThroughPipeline() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 120)
            );

            t.given(waitForCamelRouteStarted("message-history-demo", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.incoming")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "HIST-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-hist-group")
                            .message()
                            .body("""
                            {
                              "order_id": ${id},
                              "customer_id": "CUST-00${id}",
                              "item_sku": "SKU-OBS-${id}",
                              "quantity": 1,
                              "amount": ${amount},
                              "validated": true,
                              "shipping_priority": "STANDARD"
                            }
                            """)
                    )
            );
        }
    }

    @Nested
    @Order(3)
    class MessageStoreTest {

        @Test
        public void shouldStoreMessageInDatabase() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 200)
            );

            t.given(waitForCamelRouteStarted("message-store-example", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.incoming")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header("kafka.KEY", "STORE-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        sql(dataSource)
                            .query()
                            .statement("SELECT COUNT(*) AS cnt FROM system.message_store WHERE payload LIKE '%SKU-OBS-${id}%'")
                            .validate("cnt", "1")
                    )
            );
        }
    }

}
