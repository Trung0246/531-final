package com.datasetviz.dto;

import java.util.List;

public record DashboardProgressEvent(
        String datasetId,
        String stage,
        String message,
        int scannedFiles,
        int totalFiles,
        int processedRows,
        int failedFiles,
        List<FileProgress> files,
        DashboardView dashboard,
        boolean complete
) {

    public record FileProgress(String path, String name, String status, int processedRows, String message) {
    }
}
