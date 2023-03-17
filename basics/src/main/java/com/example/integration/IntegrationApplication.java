package com.example.integration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.SystemPropertyUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class IntegrationApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(IntegrationApplication.class, args);
        Thread.currentThread().join();
    }

    @Bean
    MessageChannel requests() {
        return MessageChannels.direct().get();
    }

    @Bean
    DirectChannel replies() {
        return MessageChannels.direct().get();
    }


    @Bean
    IntegrationFlow inboundFlow() {
        var directory = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/in"));
        var files = Files.inboundAdapter(directory).autoCreateDirectory(true);
        return IntegrationFlow
                .from(files, poller -> poller.poller(pm -> pm.fixedRate(1, TimeUnit.SECONDS)))
                .transform(new FileToStringTransformer())
                .handle((GenericHandler<String>) (payload, headers) -> {
                    System.out.println("start of the line");
                    headers.forEach((key, value) -> System.out.println(key + '=' + value));
                    return payload;
                })
                .channel(requests())
                .get();
    }

    @Bean
    IntegrationFlow flow() {
        return IntegrationFlow
                .from(requests())
                .filter(String.class, source -> source.contains("hola"))
                .transform((GenericTransformer<String, String>) String::toUpperCase)
                .channel(replies())
                .get();
    }

    @Bean
    IntegrationFlow outboundFlow() {
        var directory = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/out"));
        return IntegrationFlow
                .from(replies())
                .handle((GenericHandler<String>) (payload, headers) -> {
                    System.out.println("end of the line");
                    headers.forEach((key, value) -> System.out.println(key + '=' + value));
                    return payload;
                })
                .handle(Files.outboundAdapter(directory).autoCreateDirectory(true))
                .get();
    }

}
