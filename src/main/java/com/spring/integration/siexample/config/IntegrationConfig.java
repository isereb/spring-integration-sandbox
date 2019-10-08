package com.spring.integration.siexample.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.PriorityChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.GenericSelector;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.messaging.MessageHandler;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.io.File;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@Slf4j
@EnableIntegration
@Configuration
public class IntegrationConfig {

    @Bean public IntegrationFlow flow() {
        log.info("Starting integration flow");
        return IntegrationFlows
                .from(sourceDirectory(), conf -> conf.poller(Pollers.fixedDelay(10000)))
                .filter(onlyTxts())
                .channel(alphabetically())
                .handle(targetDirectory())
                .get();
    }

    @Bean
    public PriorityChannel alphabetically() {
        return new PriorityChannel(1000, Comparator.comparing(left -> ((File) left.getPayload()).getName()));
    }

    @Bean public MessageSource<File> sourceDirectory() {
        FileReadingMessageSource messageSource = new FileReadingMessageSource();
        messageSource.setDirectory(new File("input_dir"));
        return messageSource;
    }

    @Bean public MessageHandler targetDirectory() {
        FileWritingMessageHandler handler = new FileWritingMessageHandler(new File("output_dir"));
        handler.setFileExistsMode(FileExistsMode.REPLACE);
        handler.setExpectReply(false);
        return handler;
    }

    @Bean
    public GenericSelector<File> onlyTxts() {
        return source -> source.getName()
                .endsWith(".txt");
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(10, TimeUnit.SECONDS));
        return pollerMetadata;
    }
}
