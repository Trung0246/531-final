package com.example.datasetviz;

import com.example.datasetviz.config.AnalyticsProperties;
import com.example.datasetviz.config.HdfsConfiguration;
import com.example.datasetviz.config.HdfsProperties;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;
import org.springframework.beans.factory.ObjectProvider;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class DatasetVisualizationApplicationConfigTest {

    @Test
    void mainDelegatesToSpringApplication() {
        assertThat(new DatasetVisualizationApplication()).isNotNull();
        SpringApplication application = DatasetVisualizationApplication.createApplication();

        assertThat(application).isNotNull();
        assertThat(application.getAllSources()).contains(DatasetVisualizationApplication.class);
    }

    @Test
    void applicationIncludesJarDirectoryAsAdditionalConfigLocation() {
        assertThat(DatasetVisualizationApplication.getAdditionalConfigLocation())
                .isEqualTo("optional:file:" + DatasetVisualizationApplication.getApplicationDirectory().getAbsolutePath() + "/");
    }

    @Test
    void applicationDirectoryResolvesToExistingLocation() {
        File applicationDirectory = DatasetVisualizationApplication.getApplicationDirectory();

        assertThat(applicationDirectory).exists().isDirectory();
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
        hdfsProperties.setHdfsPath("/datasets/imports");
        hdfsProperties.setLocalPath("/srv/uploads");
        hdfsProperties.getEmbedded().setEnabled(true);
        hdfsProperties.getEmbedded().setBaseDir(new File("/tmp/hdfs"));
        hdfsProperties.getEmbedded().setDataNodes(2);
        hdfsProperties.getEmbedded().setNameNodePort(52000);
        hdfsProperties.getEmbedded().setFormat(false);
        hdfsProperties.setConfiguration(Map.of("dfs.replication", "1"));

        assertThat(hdfsProperties.getUri()).isEqualTo("hdfs://cluster:9000");
        assertThat(hdfsProperties.getUser()).isEqualTo("hadoop");
        assertThat(hdfsProperties.getHdfsPath()).isEqualTo("/datasets/imports");
        assertThat(hdfsProperties.getLocalPath()).isEqualTo("/srv/uploads");
        assertThat(hdfsProperties.getEmbedded().isEnabled()).isTrue();
        assertThat(hdfsProperties.getEmbedded().getBaseDir()).isEqualTo(new File("/tmp/hdfs"));
        assertThat(hdfsProperties.getEmbedded().getDataNodes()).isEqualTo(2);
        assertThat(hdfsProperties.getEmbedded().getNameNodePort()).isEqualTo(52000);
        assertThat(hdfsProperties.getEmbedded().isFormat()).isFalse();
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
    void hadoopConfigurationSkipsDefaultFsWhenEmbeddedEnabled() {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.getEmbedded().setEnabled(true);

        HdfsConfiguration hdfsConfiguration = new HdfsConfiguration();
        org.apache.hadoop.conf.Configuration configuration = hdfsConfiguration.hadoopConfiguration(hdfsProperties);

        assertThat(configuration.get("fs.defaultFS")).isEqualTo("file:///");
    }

    @Test
    void fileSystemUsesConfiguredUserWhenPresent() throws Exception {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setUser("hadoop");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        FileSystem fileSystem = mock(FileSystem.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<org.apache.hadoop.hdfs.MiniDFSCluster> clusterProvider = mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(null);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration, "hadoop"))
                    .thenReturn(fileSystem);

            FileSystem result = new HdfsConfiguration().fileSystem(configuration, hdfsProperties, clusterProvider);

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
        @SuppressWarnings("unchecked")
        ObjectProvider<org.apache.hadoop.hdfs.MiniDFSCluster> clusterProvider = mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(null);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration))
                    .thenReturn(fileSystem);

            FileSystem result = new HdfsConfiguration().fileSystem(configuration, hdfsProperties, clusterProvider);

            assertThat(result).isSameAs(fileSystem);
        }
    }

    @Test
    void fileSystemWrapsInterruptedException() {
        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setUri("hdfs://cluster:9000");
        hdfsProperties.setUser("hadoop");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        @SuppressWarnings("unchecked")
        ObjectProvider<org.apache.hadoop.hdfs.MiniDFSCluster> clusterProvider = mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(null);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration, "hadoop"))
                    .thenThrow(new InterruptedException("boom"));

            assertThatThrownBy(() -> new HdfsConfiguration().fileSystem(configuration, hdfsProperties, clusterProvider))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while creating HDFS FileSystem client")
                    .hasCauseInstanceOf(InterruptedException.class);

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

}
