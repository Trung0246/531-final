package com.example.datasetviz;

import com.example.datasetviz.config.AnalyticsProperties;
import com.example.datasetviz.config.HdfsConfiguration;
import com.example.datasetviz.config.HdfsProperties;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

class DatasetVisualizationApplicationConfigTest {

    @Test
    void mainDelegatesToSpringApplication() {
        assertThat(new DatasetVisualizationApplication()).isNotNull();

        try (MockedStatic<SpringApplication> springApplication = mockStatic(SpringApplication.class)) {
            springApplication.when(() -> SpringApplication.run(DatasetVisualizationApplication.class, new String[]{"arg"}))
                    .thenReturn(null);

            DatasetVisualizationApplication.main(new String[]{"arg"});

            springApplication.verify(() -> SpringApplication.run(DatasetVisualizationApplication.class, new String[]{"arg"}));
        }
    }

    @Test
    void propertiesRoundTrip() {
        AnalyticsProperties analyticsProperties = new AnalyticsProperties();
        analyticsProperties.setDefaultMaxFiles(11);
        analyticsProperties.setMaxFilesHardLimit(99);
        analyticsProperties.setCacheTtl(Duration.ofMinutes(5));
        analyticsProperties.setDefaultTopLimit(7);
        analyticsProperties.setDefaultGraphEdgeLimit(13);

        assertThat(analyticsProperties.getDefaultMaxFiles()).isEqualTo(11);
        assertThat(analyticsProperties.getMaxFilesHardLimit()).isEqualTo(99);
        assertThat(analyticsProperties.getCacheTtl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(analyticsProperties.getDefaultTopLimit()).isEqualTo(7);
        assertThat(analyticsProperties.getDefaultGraphEdgeLimit()).isEqualTo(13);

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setUser("hadoop");
        hdfsProperties.setConfiguration(Map.of("dfs.replication", "1"));

        assertThat(hdfsProperties.getUri()).isEqualTo("hdfs://cluster:9000");
        assertThat(hdfsProperties.getUser()).isEqualTo("hadoop");
        assertThat(hdfsProperties.getConfiguration()).containsEntry("dfs.replication", "1");
    }

    @Test
    void hadoopConfigurationIncludesDefaultFsAndCustomEntries() {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setConfiguration(Map.of("dfs.client.use.datanode.hostname", "true"));

        HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
        org.apache.hadoop.conf.Configuration configuration = hdfsConfiguration.hadoopConfiguration(hdfsProperties);

        assertThat(configuration.get("fs.defaultFS")).isEqualTo("hdfs://cluster:9000");
        assertThat(configuration.get("dfs.client.use.datanode.hostname")).isEqualTo("true");
    }

    @Test
    void fileSystemUsesConfiguredUserWhenPresent() throws Exception {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setUser("hadoop");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        FileSystem fileSystem = mock(FileSystem.class);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration, "hadoop"))
                    .thenReturn(fileSystem);

            FileSystem result = new HdfsConfiguration().fileSystem(configuration, hdfsProperties);

            assertThat(result).isSameAs(fileSystem);
        }
    }

    @Test
    void fileSystemUsesDefaultLookupWhenUserBlank() throws Exception {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setUser("   ");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        FileSystem fileSystem = mock(FileSystem.class);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration))
                    .thenReturn(fileSystem);

            FileSystem result = new HdfsConfiguration().fileSystem(configuration, hdfsProperties);

            assertThat(result).isSameAs(fileSystem);
        }
    }

    @Test
    void fileSystemWrapsInterruptedException() {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setUser("hadoop");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration, "hadoop"))
                    .thenThrow(new InterruptedException("boom"));

            assertThatThrownBy(() -> new HdfsConfiguration().fileSystem(configuration, hdfsProperties))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while creating HDFS FileSystem client")
                    .hasCauseInstanceOf(InterruptedException.class);

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }
}
