package com.datasetviz;

import com.datasetviz.config.AnalyticsProps;
import com.datasetviz.config.HdfsConfig;
import com.datasetviz.config.HdfsProps;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.boot.SpringApplication;
import org.springframework.beans.factory.ObjectProvider;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class AppConfigTest {

    @Test
    void mainDelegatesToSpringApplication() {
        assertThat(new App()).isNotNull();
        SpringApplication application = App.createApplication();

        assertThat(application).isNotNull();
        assertThat(application.getAllSources()).contains(App.class);
    }

    @Test
    void applicationIncludesJarDirectoryAsAdditionalConfigLocation() {
        assertThat(App.getAdditionalConfigLocation())
                .isEqualTo("optional:file:" + App.getApplicationDirectory().getAbsolutePath() + "/");
    }

    @Test
    void applicationDirectoryResolvesToExistingLocation() {
        File applicationDirectory = App.getApplicationDirectory();

        assertThat(applicationDirectory).exists().isDirectory();
    }

    @Test
    void propertiesRoundTrip() {
        AnalyticsProps analyticsProps = new AnalyticsProps();
        analyticsProps.setDefaultMaxFiles(11);
        analyticsProps.setMaxFilesHardLimit(99);
        analyticsProps.setCacheTtl(Duration.ofMinutes(5));
        analyticsProps.setDefaultTopLimit(7);
        analyticsProps.setDefaultGraphEdgeLimit(13);

        assertThat(analyticsProps.getDefaultMaxFiles()).isEqualTo(11);
        assertThat(analyticsProps.getMaxFilesHardLimit()).isEqualTo(99);
        assertThat(analyticsProps.getCacheTtl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(analyticsProps.getDefaultTopLimit()).isEqualTo(7);
        assertThat(analyticsProps.getDefaultGraphEdgeLimit()).isEqualTo(13);

        HdfsProps hdfsProps = new HdfsProps();
        hdfsProps.setUri("hdfs://cluster:9000");
        hdfsProps.setUser("hadoop");
        hdfsProps.setHdfsPath("/datasets/imports");
        hdfsProps.setLocalPath("/srv/uploads");
        hdfsProps.getEmbedded().setEnabled(true);
        hdfsProps.getEmbedded().setBaseDir(new File("/tmp/hdfs"));
        hdfsProps.getEmbedded().setDataNodes(2);
        hdfsProps.getEmbedded().setNameNodePort(52000);
        hdfsProps.getEmbedded().setFormat(false);
        hdfsProps.setConfiguration(Map.of("dfs.replication", "1"));

        assertThat(hdfsProps.getUri()).isEqualTo("hdfs://cluster:9000");
        assertThat(hdfsProps.getUser()).isEqualTo("hadoop");
        assertThat(hdfsProps.getHdfsPath()).isEqualTo("/datasets/imports");
        assertThat(hdfsProps.getLocalPath()).isEqualTo("/srv/uploads");
        assertThat(hdfsProps.getEmbedded().isEnabled()).isTrue();
        assertThat(hdfsProps.getEmbedded().getBaseDir()).isEqualTo(new File("/tmp/hdfs"));
        assertThat(hdfsProps.getEmbedded().getDataNodes()).isEqualTo(2);
        assertThat(hdfsProps.getEmbedded().getNameNodePort()).isEqualTo(52000);
        assertThat(hdfsProps.getEmbedded().isFormat()).isFalse();
        assertThat(hdfsProps.getConfiguration()).containsEntry("dfs.replication", "1");
    }

    @Test
    void hadoopConfigurationIncludesDefaultFsAndCustomEntries() {
        HdfsProps hdfsProps = new HdfsProps();
        hdfsProps.setUri("hdfs://cluster:9000");
        hdfsProps.setConfiguration(Map.of("dfs.client.use.datanode.hostname", "true"));

        HdfsConfig hdfsConfig = new HdfsConfig();
        org.apache.hadoop.conf.Configuration configuration = hdfsConfig.hadoopConfiguration(hdfsProps);

        assertThat(configuration.get("fs.defaultFS")).isEqualTo("hdfs://cluster:9000");
        assertThat(configuration.get("dfs.client.use.datanode.hostname")).isEqualTo("true");
    }

    @Test
    void hadoopConfigurationSkipsDefaultFsWhenEmbeddedEnabled() {
        HdfsProps hdfsProps = new HdfsProps();
        hdfsProps.setUri("hdfs://cluster:9000");
        hdfsProps.getEmbedded().setEnabled(true);

        HdfsConfig hdfsConfig = new HdfsConfig();
        org.apache.hadoop.conf.Configuration configuration = hdfsConfig.hadoopConfiguration(hdfsProps);

        assertThat(configuration.get("fs.defaultFS")).isEqualTo("file:///");
    }

    @Test
    void fileSystemUsesConfiguredUserWhenPresent() throws Exception {
        HdfsProps hdfsProps = new HdfsProps();
        hdfsProps.setUri("hdfs://cluster:9000");
        hdfsProps.setUser("hadoop");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        FileSystem fileSystem = mock(FileSystem.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<org.apache.hadoop.hdfs.MiniDFSCluster> clusterProvider = mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(null);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration, "hadoop"))
                    .thenReturn(fileSystem);

            FileSystem result = new HdfsConfig().fileSystem(configuration, hdfsProps, clusterProvider);

            assertThat(result).isSameAs(fileSystem);
        }
    }

    @Test
    void fileSystemUsesDefaultLookupWhenUserBlank() throws Exception {
        HdfsProps hdfsProps = new HdfsProps();
        hdfsProps.setUri("hdfs://cluster:9000");
        hdfsProps.setUser("   ");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        FileSystem fileSystem = mock(FileSystem.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<org.apache.hadoop.hdfs.MiniDFSCluster> clusterProvider = mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(null);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration))
                    .thenReturn(fileSystem);

            FileSystem result = new HdfsConfig().fileSystem(configuration, hdfsProps, clusterProvider);

            assertThat(result).isSameAs(fileSystem);
        }
    }

    @Test
    void fileSystemWrapsInterruptedException() {
        HdfsProps hdfsProps = new HdfsProps();
        hdfsProps.setUri("hdfs://cluster:9000");
        hdfsProps.setUser("hadoop");
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        @SuppressWarnings("unchecked")
        ObjectProvider<org.apache.hadoop.hdfs.MiniDFSCluster> clusterProvider = mock(ObjectProvider.class);
        when(clusterProvider.getIfAvailable()).thenReturn(null);

        try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
            fileSystemStatic.when(() -> FileSystem.get(URI.create("hdfs://cluster:9000"), configuration, "hadoop"))
                    .thenThrow(new InterruptedException("boom"));

            assertThatThrownBy(() -> new HdfsConfig().fileSystem(configuration, hdfsProps, clusterProvider))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Interrupted while creating HDFS FileSystem client")
                    .hasCauseInstanceOf(InterruptedException.class);

            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

}
