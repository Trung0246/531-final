package com.datasetviz.service;

import com.datasetviz.model.HdfsFileDescriptor;
import com.datasetviz.util.PathUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
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
public class HdfsStore {

    private final FileSystem fs;

    public HdfsStore(FileSystem fs) {
        this.fs = fs;
    }

    public boolean exists(String path) throws IOException {
        return fs.exists(new Path(PathUtils.normalizeHdfsPath(path)));
    }

    public void createDirectories(String path) throws IOException {
        fs.mkdirs(new Path(PathUtils.normalizeHdfsPath(path)));
    }

    public void copyLocalFileToHdfs(java.nio.file.Path local, String path) throws IOException {
        Path target = new Path(PathUtils.normalizeHdfsPath(path));
        Path parent = target.getParent();
        if (parent != null) {
            fs.mkdirs(parent);
        }
        fs.copyFromLocalFile(false, true, new Path(local.toUri()), target);
    }

    public void writeToHdfs(InputStream in, String path) throws IOException {
        Path target = new Path(PathUtils.normalizeHdfsPath(path));
        Path parent = target.getParent();
        if (parent != null) {
            fs.mkdirs(parent);
        }
        try (FSDataOutputStream out = fs.create(target, true)) {
            in.transferTo(out);
        }
    }

    public boolean delete(String path) throws IOException {
        return fs.delete(new Path(PathUtils.normalizeHdfsPath(path)), true);
    }

    public InputStream open(String path) throws IOException {
        return fs.open(new Path(PathUtils.normalizeHdfsPath(path)));
    }

    public List<HdfsFileDescriptor> listFiles(String path, boolean recursive, int limit) throws IOException {
        int max = Math.max(1, limit);
        List<HdfsFileDescriptor> files = new ArrayList<>();
        Path base = new Path(PathUtils.normalizeHdfsPath(path));

        if (recursive) {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(base, true);
            while (it.hasNext() && files.size() < max) {
                LocatedFileStatus stat = it.next();
                files.add(desc(stat));
            }
            return files;
        }

        FileStatus[] stats = fs.listStatus(base);
        for (FileStatus stat : stats) {
            if (files.size() >= max) {
                break;
            }
            files.add(desc(stat));
        }
        return files;
    }

    public List<String> listFilePaths(String path, int limit) throws IOException {
        int max = Math.max(1, limit);
        List<String> paths = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(PathUtils.normalizeHdfsPath(path)), true);
        while (it.hasNext() && paths.size() < max) {
            paths.add(it.next().getPath().toString());
        }
        return paths;
    }

    private HdfsFileDescriptor desc(FileStatus stat) {
        return new HdfsFileDescriptor(
                stat.getPath().toString(),
                stat.getPath().getName(),
                stat.isDirectory(),
                stat.getLen(),
                Instant.ofEpochMilli(stat.getModificationTime())
        );
    }
}
