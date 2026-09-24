package com.example.eip.aggregator;

import com.example.eip.aggregator.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.spi.Resources;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = AggregatorApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    class AggregatorTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldAggregateThreeLineItemsIntoCompleteOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-ABC-42")
                    .variable("quantity", 2)
                    .variable("price", 30)
            );

            t.given(waitForCamelRouteStarted("order-aggregator", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.line-items")
                    .message()
                    .body(Resources.create("templates/line-item.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.when(
                createVariables()
                    .variable("sku", "SKU-DEF-77")
                    .variable("quantity", 1)
                    .variable("price", 50)
            );
            t.when(
                send()
                    .endpoint("kafka:eip.orders.line-items")
                    .message()
                    .body(Resources.create("templates/line-item.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.when(
                createVariables()
                    .variable("sku", "SKU-GHI-13")
                    .variable("quantity", 3)
                    .variable("price", 20)
            );
            t.when(
                send()
                    .endpoint("kafka:eip.orders.line-items")
                    .message()
                    .body(Resources.create("templates/line-item.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.complete?consumerGroup=citrus-complete-group")
                            .message()
                            .body(Resources.create("templates/complete-order.json"))
                    )
            );
        }
    }

    @Nested
    class NormalizerPartnerATest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldNormalizePartnerAOrderToCanonicalFormat() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-PA-01")
                    .variable("quantity", 5)
                    .variable("amount", 149.95)
                    .variable("source", "PARTNER_A")
            );

            t.given(waitForCamelRouteStarted("normalizer-partner-a", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.partner-a")
                    .message()
                    .body(Resources.create("templates/partner-a-order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.normalized?consumerGroup=citrus-normalized-a-group")
                            .message()
                            .body(Resources.create("templates/normalized-order.json"))
                    )
            );
        }
    }

    @Nested
    class NormalizerPartnerBTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldNormalizePartnerBOrderToCanonicalFormat() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-PB-02")
                    .variable("quantity", 3)
                    .variable("amount", 89.97)
                    .variable("source", "PARTNER_B")
            );

            t.given(waitForCamelRouteStarted("normalizer-partner-b", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.partner-b")
                    .message()
                    .body(Resources.create("templates/partner-b-order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.normalized?consumerGroup=citrus-normalized-b-group")
                            .message()
                            .body(Resources.create("templates/normalized-order.json"))
                    )
            );
        }
    }

    @Nested
    class NormalizerPartnerCTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldNormalizePartnerCOrderToCanonicalFormat() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-PC-03")
                    .variable("quantity", 10)
                    .variable("amount", 199.90)
                    .variable("source", "PARTNER_C")
            );

            t.given(waitForCamelRouteStarted("normalizer-partner-c", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.partner-c")
                    .message()
                    .body(Resources.create("templates/partner-c-order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.normalized?consumerGroup=citrus-normalized-c-group")
                            .message()
                            .body(Resources.create("templates/normalized-order.json"))
                    )
            );
        }
    }

    @Nested
    class PersistentAggregatorTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldAggregateWithPersistentRepository() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-PA-10")
                    .variable("quantity", 1)
                    .variable("price", 25)
            );

            t.given(waitForCamelRouteStarted("persistent-order-aggregator", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.line-items")
                    .message()
                    .body(Resources.create("templates/line-item.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.when(
                createVariables()
                    .variable("sku", "SKU-PB-11")
                    .variable("quantity", 2)
                    .variable("price", 35)
            );
            t.when(
                send()
                    .endpoint("kafka:eip.orders.line-items")
                    .message()
                    .body(Resources.create("templates/line-item.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.when(
                createVariables()
                    .variable("sku", "SKU-PC-12")
                    .variable("quantity", 4)
                    .variable("price", 40)
            );
            t.when(
                send()
                    .endpoint("kafka:eip.orders.line-items")
                    .message()
                    .body(Resources.create("templates/line-item.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "ORD-${id}")
            );

            t.then(
                repeatOnError()
                    .times(15)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.complete-persistent?consumerGroup=citrus-persistent-group")
                            .message()
                            .body(Resources.create("templates/persistent-complete-order.json"))
                    )
            );
        }
    }
}
