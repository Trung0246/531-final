package com.datasetviz.util;

import java.util.Objects;

public final class PathUtils {

    private PathUtils() {
    }

    public static String normalizeHdfsPath(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().replace('\\', '/');
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    public static String resolveHdfsPath(String basePath, String relativePath) {
        String base = normalizeHdfsPath(Objects.requireNonNull(basePath, "basePath must not be null"));
        String relative = Objects.requireNonNull(relativePath, "relativePath must not be null")
                .replace('\\', '/');

        if (relative.startsWith("/")) {
            relative = relative.substring(1);
        }

        if (base.endsWith("/")) {
            return base + relative;
        }
        return base + "/" + relative;
    }

    public static String deriveMailboxOwner(String datasetRoot, String filePath) {
        String root = normalizeHdfsPath(datasetRoot);
        String file = normalizeHdfsPath(filePath);
        if (root == null || file == null || !file.startsWith(root)) {
            return "unknown";
        }

        String remainder = file.substring(root.length());
        if (remainder.startsWith("/")) {
            remainder = remainder.substring(1);
        }
        if (remainder.isBlank()) {
            return "unknown";
        }

        String[] parts = remainder.split("/");
        return parts.length == 0 || parts[0].isBlank() ? "unknown" : parts[0];
    }
}
