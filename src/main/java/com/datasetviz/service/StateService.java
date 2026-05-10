package com.datasetviz.service;

import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class StateService {

    private final ConcurrentMap<UUID, Job> jobs = new ConcurrentHashMap<>();
    private final Set<String> locks = ConcurrentHashMap.newKeySet();

    public Job start(UUID datasetId) {
        Job job = new Job(datasetId);
        Job previousJob = jobs.put(datasetId, job);
        if (previousJob != null) {
            previousJob.cancel();
        }
        return job;
    }

    public void finish(Job job) {
        jobs.remove(job.datasetId(), job);
    }

    public boolean cancel(UUID datasetId) {
        Job job = jobs.get(datasetId);
        if (job == null) {
            return false;
        }
        job.cancel();
        return true;
    }

    public boolean locked(String filePath) {
        return locks.contains(filePath);
    }

    public Lock lock(String filePath) {
        locks.add(filePath);
        return new Lock(filePath);
    }

    public final class Lock implements AutoCloseable {
        private final String filePath;

        private Lock(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void close() {
            locks.remove(filePath);
        }
    }

    public static final class Job {
        private final UUID datasetId;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        private Job(UUID datasetId) {
            this.datasetId = datasetId;
        }

        private UUID datasetId() {
            return datasetId;
        }

        private void cancel() {
            cancelled.set(true);
        }

        public void throwIfCancelled() {
            if (cancelled.get()) {
                throw new CancelledException("Dashboard analytics cancelled.");
            }
        }
    }
}
