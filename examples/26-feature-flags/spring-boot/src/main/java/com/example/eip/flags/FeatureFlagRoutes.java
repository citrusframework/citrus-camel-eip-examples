package com.example.eip.flags;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class FeatureFlagRoutes extends RouteBuilder {

    @Autowired
    FeatureFlagEvaluator flags;

    @Override
    public void configure() {

        // --- Flag-controlled Detour -------------------------------------
        // The Detour pattern from Chapter 18, with the decision moved out of
        // the code and into flagd. Flip "enrichment-enabled" in flags.json and
        // the next message takes the other path -- no rebuild, no restart.
        from("kafka:eip.orders.placed?brokers={{kafka.brokers}}&groupId=flag-detour"
                + "&autoOffsetReset=earliest")
            .routeId("feature-flag-detour")
            .unmarshal().json(Map.class)
            .setHeader("enrichmentEnabled", () -> flags.isEnabled("enrichment-enabled"))
            .choice()
                .when(header("enrichmentEnabled").isEqualTo(true))
                    .to("direct:enrich-order")
                .otherwise()
                    .to("direct:skip-enrichment")
            .end()
            .to("direct:process-order");

        from("direct:enrich-order")
            .routeId("enrich-order")
            .log("Enriching order ${body[order_id]}");

        from("direct:skip-enrichment")
            .routeId("skip-enrichment")
            .log("Enrichment disabled by feature flag for ${body[order_id]}");

        from("direct:process-order")
            .routeId("process-order")
            .log("Processing order ${body[order_id]}");

        // --- Fractional A/B routing -------------------------------------
        // "new-routing-algorithm" splits traffic 90/10 between two variants.
        // flagd hashes the targeting key to decide, so a given order always
        // lands on the same side -- the split is stable, not random per call.
        from("kafka:eip.orders.placed?brokers={{kafka.brokers}}&groupId=flag-ab-test"
                + "&autoOffsetReset=earliest")
            .routeId("feature-flag-ab-test")
            .unmarshal().json(Map.class)
            .process(exchange -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> order = exchange.getIn().getBody(Map.class);
                String algorithm = flags.getVariant(
                    "new-routing-algorithm",
                    "content-based-router",
                    Map.of("targetingKey", String.valueOf(order.get("order_id")),
                           "customer_tier", String.valueOf(order.getOrDefault("customer_tier", "STANDARD"))));
                exchange.getIn().setHeader("routingAlgorithm", algorithm);
            })
            .log("Order ${body[order_id]} -> ${header.routingAlgorithm}")
            // allowedSchemes per Chapter 14: the variant string comes from
            // outside the application, so constrain what it can resolve to.
            .toD().allowedSchemes("direct")
                .uri("direct:${header.routingAlgorithm}");

        from("direct:content-based-router")
            .routeId("algorithm-content-based-router")
            .log("  [legacy] content-based routing for ${body[order_id]}");

        from("direct:dynamic-router")
            .routeId("algorithm-dynamic-router")
            .log("  [new] dynamic routing for ${body[order_id]}");

        // --- Targeted rollout -------------------------------------------
        // "hazmat-compliance-v2" is on only for ENTERPRISE and VIP customers.
        // Same flag, different answer per message, decided by the evaluation
        // context rather than by anything in this route.
        from("kafka:eip.orders.placed?brokers={{kafka.brokers}}&groupId=flag-targeted"
                + "&autoOffsetReset=earliest")
            .routeId("feature-flag-targeted")
            .unmarshal().json(Map.class)
            .process(exchange -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> order = exchange.getIn().getBody(Map.class);
                String tier = String.valueOf(order.getOrDefault("customer_tier", "STANDARD"));
                String variant = flags.getVariant("hazmat-compliance-v2", "v1",
                    Map.of("targetingKey", String.valueOf(order.get("order_id")),
                           "customer_tier", tier));
                exchange.getIn().setHeader("hazmatVariant", variant);
                exchange.getIn().setHeader("customerTier", tier);
            })
            .log("Order ${body[order_id]} tier=${header.customerTier} "
                + "hazmat=${header.hazmatVariant}")
            .toD().allowedSchemes("direct")
                .uri("direct:orders-hazmat-${header.hazmatVariant}");

        from("direct:orders-hazmat-v1")
            .routeId("eip-orders-hazmat-v1")
            .log("  [v1] legacy hazmat compliance for ${body[order_id]}");

        from("direct:orders-hazmat-v2")
            .routeId("eip-orders-hazmat-v2")
            .log("  [v2] new hazmat compliance for ${body[order_id]}");
    }
}
