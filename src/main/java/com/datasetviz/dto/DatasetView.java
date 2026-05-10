package com.datasetviz.dto;

import com.datasetviz.model.DatasetType;

import java.util.List;

public record DatasetView(
        String id,
        String name,
        String description,
        DatasetType datasetType,
        String hdfsPath,
        String registeredAt,
        Boolean hdfsPathAlreadyExisted,
        boolean pendingLocalImport,
        List<PendingLocalImportView> pendingLocalImports
) {
    public record PendingLocalImportView(String localPath, String targetSubdirectory) {
    }
}
