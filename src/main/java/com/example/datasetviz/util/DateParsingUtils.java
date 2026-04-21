package com.example.datasetviz.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

public final class DateParsingUtils {

    private static final List<DateTimeFormatter> FORMATTERS = List.of(
            DateTimeFormatter.RFC_1123_DATE_TIME,
            DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss Z", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("d MMM yyyy HH:mm:ss Z", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("EEE, d MMM yyyy HH:mm:ss", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("d MMM yyyy HH:mm:ss", Locale.ENGLISH)
    );

    private DateParsingUtils() {
    }

    public static Optional<Instant> parseEmailDate(String rawDate) {
        if (rawDate == null || rawDate.isBlank()) {
            return Optional.empty();
        }

        String cleaned = rawDate.trim()
                .replaceAll("\\s+\\([^)]*\\)$", "")
                .replaceAll("\\s+", " ");

        for (DateTimeFormatter formatter : FORMATTERS) {
            Optional<Instant> parsed = tryParse(cleaned, formatter);
            if (parsed.isPresent()) {
                return parsed;
            }
        }

        return Optional.empty();
    }

    private static Optional<Instant> tryParse(String value, DateTimeFormatter formatter) {
        try {
            TemporalAccessor accessor = formatter.parseBest(
                    value,
                    ZonedDateTime::from,
                    LocalDateTime::from
            );

            if (accessor instanceof ZonedDateTime zonedDateTime) {
                return Optional.of(zonedDateTime.toInstant());
            }
            LocalDateTime localDateTime = (LocalDateTime) accessor;
            return Optional.of(localDateTime.toInstant(ZoneOffset.UTC));
        } catch (DateTimeParseException ignored) {
            return Optional.empty();
        }
    }
}
