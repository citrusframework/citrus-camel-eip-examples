package com.example.eip.reliability;

import com.example.eip.reliability.config.EipInfraSetup;
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

@SpringBootTest(classes = ReliabilityApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    class DeadLetterChannelRouteTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldProcessOrderSuccessfully() {
            t.given(
                createVariables()
                    .variable("id", 1001)
                    .variable("amount", 100)
                    .variable("status", "placed")
                    .variable("priority", "STANDARD")
            );

            t.given(waitForCamelRouteStarted("dead-letter-channel-demo", camelContext));

            t.given(
                print().message("Send order ${id} (not divisible by 5) → should succeed")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .message()
                    .fork(true)
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                receive()
                    .endpoint("kafka:eip.orders.processed?consumerGroup=citrus-processed-group")
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );
        }

        @Test
        public void shouldRouteFailedOrderToDLQ() {
            t.given(
                createVariables()
                    .variable("id", 1000)
                    .variable("amount", 200)
                    .variable("status", "placed")
                    .variable("priority", "EXPRESS")
            );

            t.given(waitForCamelRouteStarted("dead-letter-channel-demo", camelContext));

            t.given(
                print().message("Send order ${id} (divisible by 5) → should fail and go to DLQ")
            );

            t.when(
                send()
                    .endpoint("kafka:eip.orders.placed")
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/order.json"))
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
            );

            t.then(
                repeatOnError()
                    .times(10)
                    .actions(
                        receive()
                            .endpoint("kafka:eip.orders.dlq?consumerGroup=citrus-dlq-group")
                            .message()
                            .body(Resources.create("templates/order.json"))
                            .header(KafkaMessageHeaders.MESSAGE_KEY, "${id}")
                    )
            );
        }
    }
}
