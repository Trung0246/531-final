package com.datasetviz.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

@Configuration
@EnableConfigurationProperties({HdfsProperties.class, AnalyticsProperties.class, DatasetRegistryProperties.class})
public class HdfsConfiguration {

    @Bean
    public org.apache.hadoop.conf.Configuration hadoopConfiguration(HdfsProperties properties) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        if (!properties.getEmbedded().isEnabled()) {
            configuration.set("fs.defaultFS", properties.getUri());
        }
        properties.getConfiguration().forEach(configuration::set);
        return configuration;
    }

    @Bean(destroyMethod = "shutdown")
    public MiniDFSCluster miniDfsCluster(org.apache.hadoop.conf.Configuration configuration,
                                         HdfsProperties properties) throws IOException {
        if (!properties.getEmbedded().isEnabled()) {
            return null;
        }

        File baseDir = properties.getEmbedded().getBaseDir();
        if (baseDir == null) {
            baseDir = new File(".tmp", "datasetviz-hdfs");
        }
        Files.createDirectories(baseDir.toPath());

        File nameNodeDir = new File(baseDir, "namenode");
        File dataNodeDir = new File(baseDir, "datanode");
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        configuration.set("dfs.namenode.name.dir", nameNodeDir.toURI().toString());
        configuration.set("dfs.datanode.data.dir", dataNodeDir.toURI().toString());
        configuration.set("dfs.namenode.rpc-bind-host", "0.0.0.0");
        configuration.set("dfs.namenode.http-bind-host", "0.0.0.0");

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(configuration)
                .nameNodeHttpPort(0)
                .numDataNodes(Math.max(1, properties.getEmbedded().getDataNodes()))
                .checkExitOnShutdown(false)
                .format(properties.getEmbedded().isFormat() || !new File(nameNodeDir, "current").exists());
        if (properties.getEmbedded().getNameNodePort() > 0) {
            builder.nameNodePort(properties.getEmbedded().getNameNodePort());
        }

        MiniDFSCluster cluster = builder.build();
        cluster.waitClusterUp();
        configuration.set("fs.defaultFS", cluster.getFileSystem().getUri().toString());
        return cluster;
    }

    @Bean(destroyMethod = "close")
    public FileSystem fileSystem(org.apache.hadoop.conf.Configuration configuration,
                                 HdfsProperties properties,
                                 ObjectProvider<MiniDFSCluster> miniDfsClusterProvider) throws IOException {
        MiniDFSCluster miniDfsCluster = miniDfsClusterProvider.getIfAvailable();
        if (miniDfsCluster != null) {
            return miniDfsCluster.getFileSystem();
        }
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
