package com.example.datasetviz.util;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public final class TextAnalyticsUtils {

    private static final Set<String> STOP_WORDS = Set.of(
            "the", "and", "for", "that", "with", "this", "from", "your", "have", "will",
            "you", "are", "was", "were", "not", "can", "has", "had", "re", "fw", "fwd",
            "subject", "about", "our", "all", "out", "into", "regarding", "per", "to",
            "cc", "on", "in", "of", "at", "as", "by", "an", "be", "or", "is", "it"
    );

    private TextAnalyticsUtils() {
    }

    public static String toPreview(String body, int maxLength) {
        if (body == null || body.isBlank()) {
            return "";
        }
        String normalized = body.replaceAll("\\s+", " ").trim();
        if (normalized.length() <= maxLength) {
            return normalized;
        }
        return normalized.substring(0, Math.max(0, maxLength - 3)) + "...";
    }

    public static List<String> subjectKeywords(String subject) {
        if (subject == null || subject.isBlank()) {
            return List.of();
        }

        return List.of(subject.toLowerCase(Locale.ROOT).split("[^a-z0-9]+"))
                .stream()
                .map(String::trim)
                .filter(token -> !token.isBlank())
                .filter(token -> token.length() >= 3)
                .filter(token -> token.chars().anyMatch(Character::isLetter))
                .filter(token -> !STOP_WORDS.contains(token))
                .collect(Collectors.toList());
    }
}
