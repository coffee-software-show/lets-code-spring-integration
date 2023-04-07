package com.example.rabbitmqstreams;

import com.rabbitmq.stream.Environment;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.RabbitStream;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;

import java.util.Map;

import static com.example.rabbitmqstreams.RabbitmqStreamsApplication.payload;

@SpringBootApplication
public class RabbitmqStreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitmqStreamsApplication.class, args);
    }

    static Map<String, String> payload(String name) {
        return Map.of("message", "Hello, " + name + "!");
    }
}

@Configuration
class RabbitMqStreamsConfiguration {

    @Bean
    MessageChannel streamMessageChannel() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow inboundRabbitStreamIntegrationFlow(
            RabbitProperties rabbitProperties, com.rabbitmq.stream.Environment environment) {
        return IntegrationFlow
                .from(RabbitStream.inboundAdapter(environment)
                        .configureContainer(container -> container.queueName(rabbitProperties.getStream().getName())))
                .handle((payload, headers) -> {
                    System.out.println("got the stream payload " + payload);
                    return null;
                })
                .get();
    }

    @Bean
    InitializingBean initializingBean(RabbitProperties rabbitProperties, Environment environment) {
        return () -> environment.streamCreator().stream(rabbitProperties.getStream().getName()).create();
    }

    @Bean
    ApplicationRunner rabbitmqStreamInitializer( ) {
        return (arguments) -> streamMessageChannel()
                .send(org.springframework.messaging.support.MessageBuilder.withPayload(
                        payload("Integration Stream")).build());
    }

    @Bean
    IntegrationFlow outboundRabbitStreamIntegrationFlow(RabbitStreamTemplate template) {
        return IntegrationFlow
                .from(streamMessageChannel())
                .handle(RabbitStream.outboundStreamAdapter(template))
                .get();
    }


}
