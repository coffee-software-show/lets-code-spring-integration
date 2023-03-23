package com.example.ftpintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.ftp.dsl.Ftp;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;

import java.io.File;

@Slf4j
@SpringBootApplication
public class FtpIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtpIntegrationApplication.class, args);
    }

    @Bean
    DefaultFtpSessionFactory defaultFtpSessionFactory() {
        var ftpSessionFactory = new DefaultFtpSessionFactory();
        ftpSessionFactory.setHost("localhost");
        ftpSessionFactory.setPassword("pw");
        ftpSessionFactory.setUsername("jlong");
        return ftpSessionFactory;
    }

    @Bean
    IntegrationFlow inboundFileFlow(
            @Value("${HOME}/Desktop/ftp-client-integration/local") File ftpLocal,
            @Value("${HOME}/Desktop/ftp-client-integration/out") File out,
            DefaultFtpSessionFactory ftpSessionFactory
    ) {
        var inbound = Ftp
                .inboundAdapter(ftpSessionFactory)
                .autoCreateLocalDirectory(true)
                .localDirectory(ftpLocal)
                .deleteRemoteFiles(true)
                .get();
        var outbound = Ftp
                .outboundAdapter(ftpSessionFactory)
                .autoCreateDirectory(true)
                .temporaryRemoteDirectory("temp") // warning: make sure to create the remote directories yourself!
                .remoteDirectory("processed")
                .fileNameGenerator(message -> Long.toString(System.currentTimeMillis()))
                .get();
        return IntegrationFlow
                .from(inbound)
                .filter(File.class, source -> source.isFile() && source.getName().endsWith(".csv"))
                .handle((GenericHandler<File>) (payload, headers) -> {
                    log.info("payload: " + payload.getAbsolutePath());
                    headers.forEach((k, v) -> System.out.println(k + "=" + v));
                    return payload;
                })
                .handle(outbound)
                .get();
    }
}
