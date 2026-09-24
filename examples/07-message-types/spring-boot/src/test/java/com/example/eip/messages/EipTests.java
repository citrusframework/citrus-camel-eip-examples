package com.example.eip.messages;

import com.example.eip.messages.config.EipInfraSetup;
import org.apache.camel.CamelContext;
import org.apache.camel.test.spring.junit5.CamelSpringBootTest;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.camel.endpoint.CamelEndpointBuilder;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.spi.Resources;
import org.citrusframework.spring.config.CitrusSpringConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest(classes = MessageTypesApplication.class)
@CamelSpringBootTest
@CitrusSpringSupport
@ContextConfiguration(classes = { EipInfraSetup.class, CitrusSpringConfig.class })
class EipTests implements EipTestSupport {

    @Autowired
    CamelContext camelContext;

    @Nested
    class CommandMessageTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldPublishCommandToKafka() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 99.95)
            );

            t.given(waitForCamelRouteStarted("command-message-consumer", camelContext));

            t.given(
                print().message("Send command message via direct → Kafka")
            );

            t.when(
                camel()
                    .send()
                    .endpoint(new CamelEndpointBuilder()
                            .endpointUri("direct:send-command")
                            .camelContext(camelContext)
                            .build())
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/command.json"))
                    .header("kafka.KEY", "PAY-${id}")
            );

            t.then(
                receive()
                    .endpoint("kafka:eip.commands.process-payment?consumerGroup=citrus-command-group")
                    .message()
                    .body(Resources.create("templates/command.json"))
            );
        }
    }

    @Nested
    class DocumentMessageTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldPublishDocumentToKafka() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 149.99)
            );

            t.given(waitForCamelRouteStarted("document-message-consumer", camelContext));

            t.given(
                print().message("Send document message via direct → Kafka")
            );

            t.when(
                camel()
                    .send()
                    .endpoint(new CamelEndpointBuilder()
                            .endpointUri("direct:send-document")
                            .camelContext(camelContext)
                            .build())
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/document.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                receive()
                    .endpoint("kafka:eip.documents.orders?consumerGroup=citrus-document-group")
                    .message()
                    .body(Resources.create("templates/document.json"))
            );
        }
    }

    @Nested
    class EventMessageTest {

        @CitrusResource
        TestCaseRunner t;

        @Test
        public void shouldPublishEventToKafka() {
            t.given(
                createVariables()
                    .variable("id", "citrus:randomNumber(4)")
                    .variable("amount", 75.50)
            );

            t.given(waitForCamelRouteStarted("event-message-consumer", camelContext));

            t.given(
                print().message("Send event message via direct → Kafka")
            );

            t.when(
                camel()
                    .send()
                    .endpoint(new CamelEndpointBuilder()
                            .endpointUri("direct:send-event")
                            .camelContext(camelContext)
                            .build())
                    .fork(true)
                    .message()
                    .body(Resources.create("templates/event.json"))
                    .header("kafka.KEY", "${id}")
            );

            t.then(
                receive()
                    .endpoint("kafka:eip.events.orders?consumerGroup=citrus-event-group")
                    .message()
                    .body(Resources.create("templates/event.json"))
            );
        }
    }
}
