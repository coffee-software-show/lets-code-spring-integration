package com.example.ftpintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.core.GenericSelector;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.FileListFilter;
import org.springframework.integration.file.filters.IgnoreHiddenFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;

import java.io.File;
import java.util.Set;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class FtpIntegrationApplication {

    public static void main(String[] args) {
        SpringApplication.run(FtpIntegrationApplication.class, args);
    }

    @Bean
    IntegrationFlow inboundFileFlow(
            @Value("${HOME}/Desktop/in") File in,
            @Value("${HOME}/Desktop/out") File out
    ) {
        var inbound = Files
                .inboundAdapter(in)
                .autoCreateDirectory(true)
                .get();
        var outbound = Files.outboundAdapter(out)
                .autoCreateDirectory(true)
                .fileNameGenerator(message -> Long.toString(System.currentTimeMillis()))
                .fileExistsMode(FileExistsMode.FAIL)
                .deleteSourceFiles(true)
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
