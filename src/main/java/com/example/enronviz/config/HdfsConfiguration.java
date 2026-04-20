package com.example.enronviz.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.net.URI;

@Configuration
@EnableConfigurationProperties({HdfsProperties.class, AnalyticsProperties.class})
public class HdfsConfiguration {

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration(HdfsProperties properties) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        configuration.set("fs.defaultFS", properties.getUri());
        properties.getConfiguration().forEach(configuration::set);
        return configuration;
    }

    @Bean(destroyMethod = "close")
    public FileSystem fileSystem(org.apache.hadoop.conf.Configuration configuration,
                                 HdfsProperties properties) throws IOException {
        try {
            URI uri = URI.create(properties.getUri());
            if (StringUtils.hasText(properties.getUser())) {
                return FileSystem.get(uri, configuration, properties.getUser());
            }
            return FileSystem.get(uri, configuration);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while creating HDFS FileSystem client", exception);
        }
    }
}
