package com.example.datasetviz.dto;

import com.example.datasetviz.model.DatasetType;

public record DatasetView(
        String id,
        String name,
        String description,
        DatasetType datasetType,
        String hdfsPath,
        String registeredAt
) {
}
