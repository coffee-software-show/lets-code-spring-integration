package com.example.integration;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;


@SpringBootApplication
public class IntegrationApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(IntegrationApplication.class, args);
        Thread.currentThread().join();
    }

    private final Map<Integer, Order> ordersDb = new ConcurrentHashMap<>();

    @Bean
    ApplicationRunner runner(MessageChannel orders) {
        return event -> {
            var payload = new Order(1, Set.of(new LineItem("1"), new LineItem("2"), new LineItem("3")));
            this.ordersDb.put(payload.id(), payload);
            var orderMessage = MessageBuilder.withPayload(payload)
                    .setHeader("orderId", payload.id())
                    .build();
            orders.send(orderMessage);
        };
    }

    @Bean
    MessageChannel loggingChannel() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow loggingFlow() {
        return IntegrationFlow
                .from(loggingChannel())
                .handle((payload, headers) -> {
                    System.out.println("tapping " + payload);
                    headers.forEach((k, v) -> System.out.println(k + '=' + v));
                    return null;
                })
                .get();
    }

    @Bean
    IntegrationFlow etailerFlow() {

        return IntegrationFlow
                .from(orders())
                .split((Function<Order, Collection<LineItem>>) Order::lineItems)
                .wireTap(loggingChannel())
                .aggregate()
                .handle((payload, headers) -> {
                    System.out.println("after the aggregation: " + payload);
                    return null;
                })
                .get();

    }

    @Bean
    MessageChannel orders() {
        return MessageChannels.direct().get();
    }

}

record Order(Integer id, Set<LineItem> lineItems) {
}

record LineItem(String sku) {
}