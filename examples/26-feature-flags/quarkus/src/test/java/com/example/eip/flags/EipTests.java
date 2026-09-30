package com.example.eip.flags;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.quarkus.CitrusSupport;
import org.citrusframework.spi.BindToRegistry;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.api.Assertions;
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
    class FeatureFlagDetourTest {

        @Test
        public void shouldEnrichOrderWhenFlagEnabled() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("tier", "STANDARD")
                    .variable("amount", 150)
            );

            t.given(waitForCamelRouteStarted("feature-flag-detour", camelContext));
            t.given(resetRouteStats(camelContext,
                    "feature-flag-detour", "enrich-order", "skip-enrichment", "process-order"));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "DETOUR-${id}")
            );

            t.then(verifyCompletedExchanges("feature-flag-detour", 1, camelContext));
            t.then(verifyCompletedExchanges("enrich-order", 1, camelContext));
            t.then(verifyCompletedExchanges("process-order", 1, camelContext));
        }
    }

    @Nested
    @Order(2)
    class FeatureFlagAbTestTest {

        @Test
        public void shouldRouteOrderViaFractionalFlag() {
            t.given(
                createVariables()
                    .variable("tier", "STANDARD")
                    .variable("amount", 200)
            );

            t.given(waitForCamelRouteStarted("feature-flag-ab-test", camelContext));
            t.given(resetRouteStats(camelContext,
                    "feature-flag-ab-test", "algorithm-content-based-router", "algorithm-dynamic-router"));

            t.when(
                iterate()
                    .times(10)
                    .actions(
                        createVariables()
                                .variable("id", "citrus:randomNumber(4)"),
                        send()
                            .endpoint("kafka:eip.orders.placed")
                            .message()
                            .fork(true)
                            .body(Resources.create("templates/order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "AB-${id}")
                    )
            );

            t.then(verifyCompletedExchanges("feature-flag-ab-test", 10, camelContext));
            t.then(verifyRouteStats("algorithm-content-based-router", """
            { "exchangesCompleted": "@variable(algorithm-legacy-count)@" }
            """, camelContext));
            t.then(verifyRouteStats("algorithm-dynamic-router", """
            { "exchangesCompleted": "@variable(algorithm-new-count)@" }
            """, camelContext));

            t.then(context -> {
                int algorithmLegacyCount = context.getVariable("algorithm-legacy-count", Integer.class);
                int algorithmNewCount = context.getVariable("algorithm-new-count", Integer.class);

                Assertions.assertEquals(10, algorithmLegacyCount + algorithmNewCount);
                Assertions.assertTrue(algorithmNewCount > 0);
            });
        }
    }

    @Nested
    @Order(3)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class FeatureFlagTargetedTest {

        @Test
        @Order(1)
        public void shouldEvaluateTargetedFlagForStandardTier() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("tier", "STANDARD")
                    .variable("amount", 100)
            );

            t.given(waitForCamelRouteStarted("feature-flag-targeted", camelContext));
            t.given(resetRouteStats(camelContext, "feature-flag-targeted"));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "TARGETED-STD-${id}")
            );

            t.then(verifyCompletedExchanges("feature-flag-targeted", 1, camelContext));
        }

        @Test
        @Order(2)
        public void shouldEvaluateTargetedFlagForEnterpriseTier() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("tier", "ENTERPRISE")
                    .variable("amount", 500)
            );

            t.given(resetRouteStats(camelContext, "feature-flag-targeted"));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "TARGETED-ENT-${id}")
            );

            t.then(verifyCompletedExchanges("feature-flag-targeted", 1, camelContext));
        }

        @Test
        @Order(3)
        public void shouldEvaluateTargetedFlagForVipTier() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("tier", "VIP")
                    .variable("amount", 250)
            );

            t.given(resetRouteStats(camelContext, "feature-flag-targeted"));

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "TARGETED-VIP-${id}")
            );

            t.then(verifyCompletedExchanges("feature-flag-targeted", 1, camelContext));
        }
    }
}
