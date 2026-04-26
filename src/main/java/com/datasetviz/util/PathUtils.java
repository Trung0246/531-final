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
        if (normalized.contains("://")) {
            int authorityStart = normalized.indexOf("://") + 3;
            int pathStart = normalized.indexOf('/', authorityStart);
            normalized = pathStart >= 0 ? normalized.substring(pathStart) : "/";
        }
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    public static String resolveHdfsPath(String basePath, String relativePath) {
        String base = normalizeHdfsPath(Objects.requireNonNull(basePath, "basePath must not be null"));
        String relative = validateRelativePath(relativePath);

        if (relative.startsWith("/")) {
            relative = relative.substring(1);
        }

        if (relative.isBlank()) {
            return base;
        }

        if (base.endsWith("/")) {
            return base + relative;
        }
        return base + "/" + relative;
    }

    public static String resolveDatasetFilePath(String datasetRoot, String requestedPath) {
        String root = normalizeHdfsPath(Objects.requireNonNull(datasetRoot, "datasetRoot must not be null"));
        String requested = normalizeHdfsPath(requestedPath);
        if (requested == null || requested.isBlank()) {
            return root;
        }

        validatePathSegments(requested);
        if (requested.equals(root) || requested.startsWith(root + "/")) {
            return requested;
        }

        return resolveHdfsPath(root, requested);
    }

    public static String requireDatasetChildPath(String datasetRoot, String requestedPath) {
        String root = normalizeHdfsPath(Objects.requireNonNull(datasetRoot, "datasetRoot must not be null"));
        String resolved = resolveDatasetFilePath(root, requestedPath);
        if (resolved.equals(root)) {
            throw new IllegalArgumentException("File path must point to a file under dataset root");
        }
        if (!resolved.startsWith(root + "/")) {
            throw new IllegalArgumentException("File path must stay under dataset root: " + root);
        }
        return resolved;
    }

    private static String validateRelativePath(String value) {
        String relative = Objects.requireNonNull(value, "relativePath must not be null").replace('\\', '/');
        validatePathSegments(relative);
        return relative;
    }

    private static void validatePathSegments(String value) {
        if (value.contains("://")) {
            throw new IllegalArgumentException("HDFS file paths must not include a URI scheme");
        }
        for (String segment : value.split("/")) {
            if (segment.equals("..")) {
                throw new IllegalArgumentException("HDFS file paths must not contain '..'");
            }
        }
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
