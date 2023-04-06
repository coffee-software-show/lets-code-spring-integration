package com.example.rabbitmqbasics;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Map;

@SpringBootApplication
public class RabbitmqBasicsApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RabbitmqBasicsApplication.class, args);
        Thread.currentThread().join();
    }

    private final static String REQUESTS_NAME = "requests";

    @Bean
    MessageChannel requests() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow rabbitProducerFlow(AmqpTemplate template) {
        return IntegrationFlow
                .from(this.requests())
                .handle(Amqp.outboundAdapter(template).exchangeName(REQUESTS_NAME).routingKey(REQUESTS_NAME).get())
                .get();
    }

    @Bean
    ApplicationRunner runner(MessageChannel requests) {
        return args -> requests.send(MessageBuilder.withPayload(Map.of("message", "Hello, world!")).build());
    }

/*    @Bean
    ApplicationRunner producer(AmqpTemplate template) {
        return args -> {
            var message = MessageBuilder
                    .withBody("hello world".getBytes(StandardCharsets.UTF_8))
                    .build();
            template.send(REQUESTS_NAME, REQUESTS_NAME, message);
        };
    }*/


    @Bean
    IntegrationFlow rabbitConsumerFlow(ConnectionFactory connectionFactory, Queue queue) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, queue).get())
                .handle((GenericHandler<Map<String, String>>) (payload, headers) -> {
                    headers.forEach((k, v) -> System.out.println(k + '=' + v));
                    System.out.println("got a new message: " + payload);
                    return null;
                })
                .get();
    }

    /*@RabbitListener(queues = REQUESTS_NAME)
    public void incomingRequests(@Payload Map<String, String> payload) {
        System.out.println("got a new message: " + payload);
    }
*/
    @Bean
    Binding binding() {
        return BindingBuilder
                .bind(this.queue())
                .to(this.exchange())
                .with(REQUESTS_NAME)
                .noargs();
    }

    @Bean
    Queue queue() {
        return QueueBuilder
                .durable(REQUESTS_NAME)
                .build();
    }

    @Bean
    Exchange exchange() {
        return ExchangeBuilder
                .directExchange(REQUESTS_NAME)
                .build();
    }
}


// producer -> exchange -> | exchange -> queue | excange -> queue | queue <- consumer
// jakarta.jms.Producer -> jakarta.jms.Destination <- jakarta.jms.Consumer