package com.example.datasetviz.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EmailAddressUtils {

    private static final Pattern EMAIL_PATTERN = Pattern.compile(
            "[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}",
            Pattern.CASE_INSENSITIVE
    );

    private EmailAddressUtils() {
    }

    public static Optional<String> normalizeSingleAddress(String rawHeaderValue) {
        List<String> addresses = normalizeAddresses(rawHeaderValue);
        if (addresses.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(addresses.get(0));
    }

    public static List<String> normalizeAddresses(String rawHeaderValue) {
        if (rawHeaderValue == null || rawHeaderValue.isBlank()) {
            return List.of();
        }

        Set<String> addresses = new LinkedHashSet<>();
        Matcher matcher = EMAIL_PATTERN.matcher(rawHeaderValue);
        while (matcher.find()) {
            addresses.add(matcher.group().toLowerCase(Locale.ROOT));
        }

        if (!addresses.isEmpty()) {
            return new ArrayList<>(addresses);
        }

        String[] parts = rawHeaderValue.split("[,;]");
        for (String part : parts) {
            String candidate = part.trim().toLowerCase(Locale.ROOT);
            if (!candidate.isBlank()) {
                addresses.add(candidate);
            }
        }
        return new ArrayList<>(addresses);
    }
}
