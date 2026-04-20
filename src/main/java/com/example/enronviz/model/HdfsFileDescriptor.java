package com.example.enronviz.model;

import java.time.Instant;

public class HdfsFileDescriptor {

    private String path;
    private String name;
    private boolean directory;
    private long length;
    private Instant modificationTime;

    public HdfsFileDescriptor() {
    }

    public HdfsFileDescriptor(String path,
                              String name,
                              boolean directory,
                              long length,
                              Instant modificationTime) {
        this.path = path;
        this.name = name;
        this.directory = directory;
        this.length = length;
        this.modificationTime = modificationTime;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDirectory() {
        return directory;
    }

    public void setDirectory(boolean directory) {
        this.directory = directory;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public Instant getModificationTime() {
        return modificationTime;
    }

    public void setModificationTime(Instant modificationTime) {
        this.modificationTime = modificationTime;
    }
}
