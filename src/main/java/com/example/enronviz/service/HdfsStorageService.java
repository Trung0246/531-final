package com.example.enronviz.service;

import com.example.enronviz.model.HdfsFileDescriptor;
import com.example.enronviz.util.PathUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
public class HdfsStorageService {

    private final FileSystem fileSystem;

    public HdfsStorageService(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public boolean exists(String hdfsPath) throws IOException {
        return fileSystem.exists(new Path(PathUtils.normalizeHdfsPath(hdfsPath)));
    }

    public void createDirectories(String hdfsPath) throws IOException {
        fileSystem.mkdirs(new Path(PathUtils.normalizeHdfsPath(hdfsPath)));
    }

    public void copyLocalFileToHdfs(java.nio.file.Path localPath, String hdfsPath) throws IOException {
        Path targetPath = new Path(PathUtils.normalizeHdfsPath(hdfsPath));
        Path parent = targetPath.getParent();
        if (parent != null) {
            fileSystem.mkdirs(parent);
        }
        fileSystem.copyFromLocalFile(false, true, new Path(localPath.toUri()), targetPath);
    }

    public InputStream open(String hdfsPath) throws IOException {
        return fileSystem.open(new Path(PathUtils.normalizeHdfsPath(hdfsPath)));
    }

    public List<HdfsFileDescriptor> listFiles(String hdfsPath, boolean recursive, int limit) throws IOException {
        int safeLimit = Math.max(1, limit);
        List<HdfsFileDescriptor> files = new ArrayList<>();
        Path basePath = new Path(PathUtils.normalizeHdfsPath(hdfsPath));

        if (recursive) {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(basePath, true);
            while (iterator.hasNext() && files.size() < safeLimit) {
                LocatedFileStatus status = iterator.next();
                files.add(toDescriptor(status));
            }
            return files;
        }

        FileStatus[] statuses = fileSystem.listStatus(basePath);
        for (FileStatus status : statuses) {
            if (files.size() >= safeLimit) {
                break;
            }
            files.add(toDescriptor(status));
        }
        return files;
    }

    public List<String> listFilePaths(String hdfsPath, int limit) throws IOException {
        int safeLimit = Math.max(1, limit);
        List<String> paths = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(PathUtils.normalizeHdfsPath(hdfsPath)), true);
        while (iterator.hasNext() && paths.size() < safeLimit) {
            paths.add(iterator.next().getPath().toString());
        }
        return paths;
    }

    private HdfsFileDescriptor toDescriptor(FileStatus status) {
        return new HdfsFileDescriptor(
                status.getPath().toString(),
                status.getPath().getName(),
                status.isDirectory(),
                status.getLen(),
                Instant.ofEpochMilli(status.getModificationTime())
        );
    }
}
