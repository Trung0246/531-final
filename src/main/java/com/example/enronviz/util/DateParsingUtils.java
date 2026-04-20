package com.example.enronviz.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
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
            try {
                TemporalAccessor accessor = formatter.parseBest(
                        cleaned,
                        ZonedDateTime::from,
                        OffsetDateTime::from,
                        LocalDateTime::from
                );

                if (accessor instanceof ZonedDateTime zonedDateTime) {
                    return Optional.of(zonedDateTime.toInstant());
                }
                if (accessor instanceof OffsetDateTime offsetDateTime) {
                    return Optional.of(offsetDateTime.toInstant());
                }
                if (accessor instanceof LocalDateTime localDateTime) {
                    return Optional.of(localDateTime.toInstant(ZoneOffset.UTC));
                }
            } catch (DateTimeParseException ignored) {
                // Try next formatter.
            }
        }

        return Optional.empty();
    }
}
