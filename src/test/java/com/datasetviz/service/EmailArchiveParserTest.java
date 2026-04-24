package com.datasetviz.service;

import com.datasetviz.model.EmailRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class EmailArchiveParserTest {

    private final EmailArchiveParser parser = new EmailArchiveParser();

    @Test
    void parsesBasicEmailHeaders() throws Exception {
        String rawEmail = "Message-ID: <1@test>\n"
                + "Date: Mon, 14 May 2001 16:39:00 -0700 (PDT)\n"
                + "From: john.doe@example.com\n"
                + "To: jane.doe@example.com, bob@example.com\n"
                + "Subject: Trading update\n"
                + "\n"
                + "Body text.";

        EmailRecord record = parser.parse(
                        new ByteArrayInputStream(rawEmail.getBytes(StandardCharsets.UTF_8)),
                        "/datasets/mail-archive/john/_sent_mail/1"
                )
                .orElseThrow();

        assertThat(record.getMessageId()).isEqualTo("<1@test>");
        assertThat(record.getFrom()).isEqualTo("john.doe@example.com");
        assertThat(record.getTo()).containsExactly("jane.doe@example.com", "bob@example.com");
        assertThat(record.getSubject()).isEqualTo("Trading update");
        assertThat(record.getBodyPreview()).contains("Body text");
        assertThat(record.getSentAt()).isNotNull();
    }

    @Test
    void returnsEmptyForBlankInputAndHandlesFoldedHeaders() throws Exception {
        assertThat(parser.parse(new ByteArrayInputStream("   ".getBytes(StandardCharsets.UTF_8)), "/blank"))
                .isEmpty();

        String rawEmail = "Message-ID: <2@test>\r\n"
                + "From: Example <john@example.com>\r\n"
                + "To: jane@example.com\r\n"
                + "Subject: Weekly\r\n"
                + " update\r\n"
                + "\r\n"
                + "Line one.\r\nLine two.";

        EmailRecord record = parser.parse(
                        new ByteArrayInputStream(rawEmail.getBytes(StandardCharsets.UTF_8)),
                        "/datasets/mail-archive/john/2"
                )
                .orElseThrow();

        assertThat(record.getSubject()).isEqualTo("Weekly update");
        assertThat(record.getBodyPreview()).contains("Line one. Line two.");
        assertThat(record.getFrom()).isEqualTo("john@example.com");
    }

    @Test
    void skipsInvalidHeadersAndCombinesRepeatedHeadersWithoutBody() throws Exception {
        String rawEmail = "invalid header\n"
                + "To: alpha@example.com\n"
                + "To: beta@example.com\n";

        EmailRecord record = parser.parse(
                        new ByteArrayInputStream(rawEmail.getBytes(StandardCharsets.UTF_8)),
                        "/datasets/mail-archive/john/3"
                )
                .orElseThrow();

        assertThat(record.getTo()).containsExactly("alpha@example.com", "beta@example.com");
        assertThat(record.getBodyPreview()).isEmpty();
        assertThat(record.getSubject()).isEqualTo("(no subject)");
    }
}
