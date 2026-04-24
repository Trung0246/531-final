package com.datasetviz.dto;

import com.datasetviz.model.DatasetType;

public record DatasetView(
        String id,
        String name,
        String description,
        DatasetType datasetType,
        String hdfsPath,
        String registeredAt
) {
}
