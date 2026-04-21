package com.example.datasetviz.service;

import com.example.datasetviz.dto.RegisterDatasetRequest;
import com.example.datasetviz.model.DatasetRegistration;
import com.example.datasetviz.model.DatasetType;
import com.example.datasetviz.util.PathUtils;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class DatasetRegistryService {

    private final ConcurrentMap<UUID, DatasetRegistration> datasets = new ConcurrentHashMap<>();

    public DatasetRegistration register(RegisterDatasetRequest request) {
        DatasetType datasetType = request.getDatasetType() == null ? DatasetType.EMAIL_ARCHIVE : request.getDatasetType();
        DatasetRegistration registration = new DatasetRegistration(
                UUID.randomUUID(),
                request.getName().trim(),
                request.getDescription(),
                datasetType,
                PathUtils.normalizeHdfsPath(request.getHdfsPath()),
                Instant.now()
        );
        datasets.put(registration.getId(), registration);
        return registration;
    }

    public List<DatasetRegistration> listAll() {
        List<DatasetRegistration> values = new ArrayList<>(datasets.values());
        values.sort(Comparator.comparing(DatasetRegistration::getRegisteredAt).reversed());
        return values;
    }

    public DatasetRegistration getRequired(UUID datasetId) {
        DatasetRegistration registration = datasets.get(datasetId);
        if (registration == null) {
            throw new NoSuchElementException("Dataset not found: " + datasetId);
        }
        return registration;
    }
}
