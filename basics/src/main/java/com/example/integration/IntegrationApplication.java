package com.example.integration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
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
    MessageChannel errorChannel() {
        return MessageChannels.direct().get();
    }

    @Bean
    MessageChannel errorChannelForLetterViolations() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow errorFlow() {
        return IntegrationFlow
                .from(errorChannel())
                .handle((payload, headers) -> {
                    System.out.println("inside errorFlow " + payload);
                    headers.forEach((k, v) -> System.out.println(k + '=' + v));
                    return null;
                })
                .get();
    }

    @Bean
    IntegrationFlow errorForLetterViolationsFlow() {
        return IntegrationFlow
                .from(errorChannelForLetterViolations())
                .handle((payload, headers) -> {
                    System.out.println("inside errorForLetterViolationsFlow " + payload);
                    headers.forEach((k, v) -> System.out.println(k + '=' + v));
                    return null;
                })
                .get();
    }

    @Bean
    MessageChannel uppercaseIn() {
        return MessageChannels.direct().get();
    }

    @Bean
    MessageChannel uppercaseOut() {
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
                    headers.forEach((key, value) -> System.out.println(key + '=' + value));
                    if (true)
                        throw new IllegalArgumentException("you screwed something up!")  ;
                    return payload;
                })
                .channel(uppercaseIn())
                .get();
    }

    @Bean
    IntegrationFlow uppercaseFlow() {
        return IntegrationFlow
                .from(uppercaseIn())
                .enrichHeaders(b -> b.errorChannel(errorChannelForLetterViolations()))
                .handle((GenericHandler<String>) (payload, headers) -> {
                    var good = false;
                    for (var c : payload.toCharArray())
                        if (Character.isLetter(c))
                            good = true;
                    if (!good) throw new IllegalArgumentException("you must provide some letters!");
                    return payload;
                })
                .handle((GenericHandler<String>) (payload, headers) -> payload.toUpperCase())
                .channel(uppercaseOut())
                .get();
    }


    @Bean
    IntegrationFlow outboundFlow() {
        var directory = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/out"));
        return IntegrationFlow
                .from(uppercaseOut())
                .handle((GenericHandler<String>) (payload, headers) -> {
                    System.out.println("end of the line");
                    headers.forEach((key, value) -> System.out.println(key + '=' + value));
                    return payload;
                })
                .handle(Files.outboundAdapter(directory).autoCreateDirectory(true))
                .get();
    }

}