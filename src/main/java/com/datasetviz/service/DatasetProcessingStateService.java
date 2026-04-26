package com.datasetviz.service;

import com.datasetviz.util.PathUtils;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class DatasetProcessingStateService {

    private final ConcurrentMap<UUID, ProcessingJob> activeJobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, UUID> lockedFiles = new ConcurrentHashMap<>();

    public ProcessingJob beginJob(UUID datasetId) {
        ProcessingJob job = new ProcessingJob(datasetId);
        ProcessingJob previousJob = activeJobs.put(datasetId, job);
        if (previousJob != null) {
            previousJob.cancel();
        }
        return job;
    }

    public boolean cancelJob(UUID datasetId) {
        ProcessingJob job = activeJobs.get(datasetId);
        if (job == null) {
            return false;
        }
        job.cancel();
        return true;
    }

    public void finishJob(ProcessingJob job) {
        activeJobs.remove(job.datasetId(), job);
    }

    public boolean isFileLocked(String filePath) {
        return lockedFiles.containsKey(normalize(filePath));
    }

    public FileLock lockFile(UUID datasetId, String filePath) {
        String normalizedPath = normalize(filePath);
        UUID owner = lockedFiles.putIfAbsent(normalizedPath, datasetId);
        if (owner != null) {
            throw new IllegalStateException("File is already being processed: " + filePath);
        }
        return new FileLock(normalizedPath, datasetId);
    }

    private String normalize(String filePath) {
        String normalizedPath = PathUtils.normalizeHdfsPath(filePath);
        return normalizedPath == null ? "" : normalizedPath;
    }

    public static final class ProcessingJob {
        private final UUID datasetId;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private ProcessingJob(UUID datasetId) {
            this.datasetId = datasetId;
        }

        public UUID datasetId() {
            return datasetId;
        }

        public void cancel() {
            cancelled.set(true);
        }

        public boolean isCancelled() {
            return cancelled.get() || Thread.currentThread().isInterrupted();
        }

        public void throwIfCancelled() {
            if (isCancelled()) {
                throw new AnalyticsCancelledException("Dashboard analytics cancelled.");
            }
        }
    }

    public final class FileLock implements AutoCloseable {
        private final String filePath;
        private final UUID datasetId;

        private FileLock(String filePath, UUID datasetId) {
            this.filePath = filePath;
            this.datasetId = datasetId;
        }

        @Override
        public void close() {
            lockedFiles.remove(filePath, datasetId);
        }
    }
}
