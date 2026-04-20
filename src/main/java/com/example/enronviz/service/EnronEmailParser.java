package com.example.enronviz.service;

import com.example.enronviz.model.EmailRecord;
import com.example.enronviz.util.DateParsingUtils;
import com.example.enronviz.util.EmailAddressUtils;
import com.example.enronviz.util.TextAnalyticsUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

@Service
public class EnronEmailParser {

    public Optional<EmailRecord> parse(InputStream inputStream, String filePath) throws IOException {
        String rawContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        if (rawContent.isBlank()) {
            return Optional.empty();
        }

        Map<String, String> headers = parseHeaders(rawContent);
        String body = extractBody(rawContent);

        EmailRecord record = new EmailRecord();
        record.setPath(filePath);
        record.setMessageId(headers.get("message-id"));
        record.setSentAt(DateParsingUtils.parseEmailDate(headers.get("date")).orElse(null));
        record.setFrom(EmailAddressUtils.normalizeSingleAddress(headers.get("from")).orElse(null));
        record.setTo(EmailAddressUtils.normalizeAddresses(headers.get("to")));
        record.setCc(EmailAddressUtils.normalizeAddresses(headers.get("cc")));
        record.setBcc(EmailAddressUtils.normalizeAddresses(headers.get("bcc")));
        record.setSubject(headers.getOrDefault("subject", "(no subject)").trim());
        record.setBodyPreview(TextAnalyticsUtils.toPreview(body, 280));
        return Optional.of(record);
    }

    private Map<String, String> parseHeaders(String rawContent) {
        String normalized = rawContent.replace("\r\n", "\n").replace("\r", "\n");
        String[] lines = normalized.split("\n");

        Map<String, StringBuilder> headerBuilders = new LinkedHashMap<>();
        String currentHeaderName = null;

        for (String line : lines) {
            if (line.isEmpty()) {
                break;
            }

            if ((line.startsWith(" ") || line.startsWith("\t")) && currentHeaderName != null) {
                headerBuilders.get(currentHeaderName).append(' ').append(line.trim());
                continue;
            }

            int colonIndex = line.indexOf(':');
            if (colonIndex <= 0) {
                continue;
            }

            String headerName = line.substring(0, colonIndex).trim().toLowerCase(Locale.ROOT);
            String headerValue = line.substring(colonIndex + 1).trim();

            headerBuilders.computeIfAbsent(headerName, ignored -> new StringBuilder());
            StringBuilder builder = headerBuilders.get(headerName);
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(headerValue);
            currentHeaderName = headerName;
        }

        Map<String, String> headers = new LinkedHashMap<>();
        headerBuilders.forEach((key, value) -> headers.put(key, value.toString()));
        return headers;
    }

    private String extractBody(String rawContent) {
        String normalized = rawContent.replace("\r\n", "\n").replace("\r", "\n");
        int headerSeparatorIndex = normalized.indexOf("\n\n");
        if (headerSeparatorIndex < 0) {
            return "";
        }
        return normalized.substring(headerSeparatorIndex + 2).trim();
    }
}
