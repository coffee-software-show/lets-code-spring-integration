package com.example.integration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.HeaderEnricherSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.SystemPropertyUtils;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@IntegrationComponentScan
@SpringBootApplication
public class IntegrationApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(IntegrationApplication.class, args);
        Thread.currentThread().join();
    }

    static final String REQUESTS_CHANNEL = "requests";
    static final String REPLIES_CHANNEL = "replies";

    @Bean(name = REQUESTS_CHANNEL)
    MessageChannel requests() {
        return MessageChannels.direct().get();
    }

    @Bean(name = REPLIES_CHANNEL)
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
                .enrichHeaders(spec -> spec.replyChannel(replies()))
                //  .filter(String.class, source -> source.contains("hola"))
                .transform((GenericTransformer<String, String>) String::toUpperCase)
                .handle((GenericHandler<String>) (payload, headers) -> {
                    headers.forEach((k, v) -> System.out.println(k + '=' + v));
                    return payload;
                })

               // .channel(replies())
                .get();
    }

  /*  @Bean
    IntegrationFlow fromReplies (){
        return IntegrationFlow.from( replies())
                .handle(new GenericHandler<String>() {
                    @Override
                    public Object handle(String payload, MessageHeaders headers) {
                        System.out.println("payload: " + payload);
                        return null;
                    }
                })
                .get() ;
    }*/

/*    @Bean
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
    }*/

}

@Component
class MyClientRunner {

    private final MyClient client;

    MyClientRunner(MyClient client) {
        this.client = client;
    }

    @EventListener(ApplicationReadyEvent.class)
    void ready() {
        var reply = this.client.uppercase("josh");
        System.out.println("the reply is " + reply);
    }
}

@MessagingGateway
interface MyClient {

    @Gateway(requestChannel = IntegrationApplication.REQUESTS_CHANNEL
           /* replyChannel = IntegrationApplication.REPLIES_CHANNEL*/)
    String uppercase(String input);
}
