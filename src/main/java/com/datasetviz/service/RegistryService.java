package com.datasetviz.service;

import com.datasetviz.config.RegistryProps;
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
public class RegistryService {

    private final ConcurrentMap<UUID, DatasetRegistration> datasets = new ConcurrentHashMap<>();
    private final ObjectMapper json;
    private final Path path;

    public RegistryService(ObjectMapper json, RegistryProps props) {
        this.json = json;
        this.path = props.getPath().toPath().normalize();
    }

    @PostConstruct
    public void load() throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        List<DatasetRegistration> registrations = json.readValue(
                path.toFile(),
                new TypeReference<>() {
                }
        );
        registrations.forEach(ds -> datasets.put(ds.getId(), ds));
    }

    public synchronized DatasetRegistration register(RegisterDatasetRequest request) {
        DatasetRegistration ds = new DatasetRegistration(
                UUID.randomUUID(),
                request.getName().trim(),
                request.getDescription(),
                DatasetType.GENERIC_FILES,
                PathUtils.normalizeHdfsPath(request.getHdfsPath()),
                Instant.now()
        );
        datasets.put(ds.getId(), ds);
        persist();
        return ds;
    }

    public synchronized DatasetRegistration updateDatasetType(UUID datasetId, DatasetType datasetType) {
        DatasetRegistration ds = getRequired(datasetId);
        ds.setDatasetType(datasetType == null ? DatasetType.GENERIC_FILES : datasetType);
        persist();
        return ds;
    }

    public synchronized DatasetRegistration updatePendingLocalImport(UUID datasetId, String localImportPath, String targetSubdirectory) {
        DatasetRegistration ds = getRequired(datasetId);
        ds.addPendingLocalImport(localImportPath, targetSubdirectory);
        persist();
        return ds;
    }

    public synchronized DatasetRegistration clearPendingLocalImport(UUID datasetId) {
        DatasetRegistration ds = getRequired(datasetId);
        ds.clearPendingLocalImports();
        persist();
        return ds;
    }

    public synchronized DatasetRegistration deleteDataset(UUID datasetId) {
        DatasetRegistration ds = getRequired(datasetId);
        datasets.remove(datasetId);
        persist();
        return ds;
    }

    public List<DatasetRegistration> listAll() {
        List<DatasetRegistration> values = new ArrayList<>(datasets.values());
        values.sort(Comparator.comparing(DatasetRegistration::getRegisteredAt).reversed());
        return values;
    }

    public DatasetRegistration getRequired(UUID datasetId) {
        DatasetRegistration ds = datasets.get(datasetId);
        if (ds == null) {
            throw new NoSuchElementException("Dataset not found: " + datasetId);
        }
        return ds;
    }

    private void persist() {
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempPath = path.resolveSibling(path.getFileName() + ".tmp");
            json.writerWithDefaultPrettyPrinter().writeValue(tempPath.toFile(), listAll());
            Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to persist dataset registry: " + path, exception);
        }
    }
}
