package com.example.datasetviz.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "app.hdfs")
public class HdfsProperties {

    private String uri = "hdfs://localhost:9000";
    private String user;
    private String hdfsPath;
    private String localPath;
    private Embedded embedded = new Embedded();
    private Map<String, String> configuration = new LinkedHashMap<>();

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public String getLocalPath() {
        return localPath;
    }

    public void setLocalPath(String localPath) {
        this.localPath = localPath;
    }

    public Embedded getEmbedded() {
        return embedded;
    }

    public void setEmbedded(Embedded embedded) {
        this.embedded = embedded;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public static class Embedded {

        private boolean enabled;
        private File baseDir;
        private int dataNodes = 1;
        private int nameNodePort;
        private boolean format = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public File getBaseDir() {
            return baseDir;
        }

        public void setBaseDir(File baseDir) {
            this.baseDir = baseDir;
        }

        public int getDataNodes() {
            return dataNodes;
        }

        public void setDataNodes(int dataNodes) {
            this.dataNodes = dataNodes;
        }

        public int getNameNodePort() {
            return nameNodePort;
        }

        public void setNameNodePort(int nameNodePort) {
            this.nameNodePort = nameNodePort;
        }

        public boolean isFormat() {
            return format;
        }

        public void setFormat(boolean format) {
            this.format = format;
        }
    }
}
