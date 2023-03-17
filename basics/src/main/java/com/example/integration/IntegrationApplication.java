package com.example.integration;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class IntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(IntegrationApplication.class, args);
    }

    @Bean
    MessageChannel greetings() {
        return MessageChannels.direct().get();
    }

    private static String text() {
        return Math.random() > .5 ?
                "Hello world @ " + Instant.now() + "!" :
                "hola todo el mundo @ " + Instant.now() + "!";
    }


    @Component
    static class MyMessagSource implements MessageSource<String> {

        // poller
        @Override
        public Message<String> receive() {
            return MessageBuilder.withPayload(text()).build();
        }
    }

    @Bean
    ApplicationRunner runner(MyMessagSource myMessagSource, IntegrationFlowContext context) {
        return args -> {
            var holaFlow = buildFlow(myMessagSource, 1, "hola");
            var helloFlow = buildFlow(myMessagSource, 2, "Hello");
            Set.of(helloFlow, holaFlow).forEach(flow -> context.registration(flow).register().start());
        };
    }

    private static IntegrationFlow buildFlow(MyMessagSource myMessagSource, int seconds, String filterText) {
        return IntegrationFlow
                .from(myMessagSource, p -> p.poller(pf -> pf.fixedRate(seconds, TimeUnit.SECONDS)))
                .filter(String.class, source -> source.contains(filterText))
                .transform((GenericTransformer<String, String>) String::toUpperCase)
                .handle((GenericHandler<String>) (payload, headers) -> {
                    System.out.println("the payload is for filter text [" + filterText+
                                       "] is " + payload);
                    return null;
                })
                .get();
    }


}


