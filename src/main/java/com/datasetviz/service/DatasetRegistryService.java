package com.datasetviz.service;

import com.datasetviz.config.DatasetRegistryProperties;
import com.datasetviz.dto.RegisterDatasetRequest;
import com.datasetviz.model.DatasetRegistration;
import com.datasetviz.model.DatasetType;
import com.datasetviz.util.PathUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private final ObjectMapper objectMapper;
    private final Path registryPath;

    public DatasetRegistryService(ObjectMapper objectMapper, DatasetRegistryProperties properties) {
        this.objectMapper = objectMapper;
        this.registryPath = properties.getPath().toPath().normalize();
    }

    @PostConstruct
    public void load() throws IOException {
        if (!Files.exists(registryPath)) {
            return;
        }
        List<DatasetRegistration> registrations = objectMapper.readValue(
                registryPath.toFile(),
                new TypeReference<>() {
                }
        );
        registrations.forEach(registration -> datasets.put(registration.getId(), registration));
    }

    public synchronized DatasetRegistration register(RegisterDatasetRequest request) {
        DatasetRegistration registration = new DatasetRegistration(
                UUID.randomUUID(),
                request.getName().trim(),
                request.getDescription(),
                DatasetType.GENERIC_FILES,
                PathUtils.normalizeHdfsPath(request.getHdfsPath()),
                Instant.now()
        );
        datasets.put(registration.getId(), registration);
        persist();
        return registration;
    }

    public synchronized DatasetRegistration updateDatasetType(UUID datasetId, DatasetType datasetType) {
        DatasetRegistration registration = getRequired(datasetId);
        registration.setDatasetType(datasetType == null ? DatasetType.GENERIC_FILES : datasetType);
        persist();
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

    private void persist() {
        try {
            Path parent = registryPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempPath = registryPath.resolveSibling(registryPath.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempPath.toFile(), listAll());
            Files.move(tempPath, registryPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to persist dataset registry: " + registryPath, exception);
        }
    }
}
