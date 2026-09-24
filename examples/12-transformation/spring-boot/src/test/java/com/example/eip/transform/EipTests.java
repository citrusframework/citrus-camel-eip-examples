package com.example.eip.transform;

import com.example.eip.transform.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spi.Resources;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = TransformationApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Autowired
    RedisProductCatalog redisProductCatalog;

    @Nested
    class MessageTranslatorTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldTranslateExternalOrderToCanonicalFormat() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-ABC-42")
                    .variable("quantity", 5)
                    .variable("amount", 99.95)
                    .variable("country", "US")
                    .variable("hazardous", false)
            );

            t.given(waitForCamelRouteStarted("message-translator", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.external")
                    .message()
                    .body(Resources.create("templates/external-order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.placed?consumerGroup=citrus-translator-placed-group")
                            .message()
                            .body(Resources.create("templates/canonical-order.json"))
                    )
            );
        }
    }

    @Nested
    class ContentEnricherTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldEnrichOrderWithProductCatalogData() {
            redisProductCatalog.seedCatalog();

            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-ABC-42")
                    .variable("quantity", 3)
                    .variable("amount", 89.97)
                    .variable("country", "DE")
                    .variable("hazardous", false)
                    .variable("productName", "Wireless Headphones")
                    .variable("productCategory", "Electronics")
                    .variable("weightKg", "0.3")
                    .variable("shippingZone", "ZONE-1")
            );

            t.given(waitForCamelRouteStarted("content-enricher", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/canonical-order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.enriched?consumerGroup=citrus-enriched-group")
                            .message()
                            .body(Resources.create("templates/enriched-order.json"))
                    )
            );
        }
    }

    @Nested
    class ContentFilterTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldStripNonAllowedFieldsFromOrder() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("sku", "SKU-DEF-77")
                    .variable("quantity", 1)
                    .variable("amount", 89.99)
                    .variable("country", "GB")
                    .variable("hazardous", false)
                    .variable("productName", "Running Shoes")
                    .variable("productCategory", "Footwear")
                    .variable("weightKg", "1.2")
                    .variable("shippingZone", "ZONE-2")
            );

            t.given(waitForCamelRouteStarted("content-filter", camelContext));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.enriched")
                    .message()
                    .body(Resources.create("templates/enriched-order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.analytics?consumerGroup=citrus-analytics-group")
                            .message()
                            .body(Resources.create("templates/filtered-order.json"))
                    )
            );
        }
    }
}
