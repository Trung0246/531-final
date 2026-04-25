package com.datasetviz.dto;

public record DashboardProgressEvent(
        String datasetId,
        String stage,
        String message,
        int scannedFiles,
        int totalFiles,
        int processedRows,
        int failedFiles,
        boolean complete
) {
}
