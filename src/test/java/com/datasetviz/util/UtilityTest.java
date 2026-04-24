package com.datasetviz.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class UtilityTest {

    @Test
    void pathUtilsHandleNormalizationResolutionAndMailboxOwner() {
        assertThat(PathUtils.normalizeHdfsPath(null)).isNull();
        assertThat(PathUtils.normalizeHdfsPath("/datasets/mail///")).isEqualTo("/datasets/mail");
        assertThat(PathUtils.normalizeHdfsPath("\\datasets\\mail\\")).isEqualTo("/datasets/mail");
        assertThat(PathUtils.resolveHdfsPath("/datasets/base/", "/child/file.txt")).isEqualTo("/datasets/base/child/file.txt");
        assertThat(PathUtils.resolveHdfsPath("/", "child/file.txt")).isEqualTo("/child/file.txt");
        assertThat(PathUtils.deriveMailboxOwner("/datasets/mail", "/datasets/mail/john/sent/1")).isEqualTo("john");
        assertThat(PathUtils.deriveMailboxOwner("/datasets/mail", "/outside/file.txt")).isEqualTo("unknown");
        assertThat(PathUtils.deriveMailboxOwner("/datasets/mail", "/datasets/mail")).isEqualTo("unknown");
        assertThat(PathUtils.deriveMailboxOwner("/datasets/mail", "/datasets/mail//child")).isEqualTo("unknown");
    }

    @Test
    void emailAddressUtilsHandleRegexAndFallbackParsing() {
        assertThat(EmailAddressUtils.normalizeSingleAddress("John <john@example.com>")).contains("john@example.com");
        assertThat(EmailAddressUtils.normalizeSingleAddress("   ")).isEmpty();
        assertThat(EmailAddressUtils.normalizeAddresses("John <john@example.com>, jane@example.com, JANE@example.com"))
                .containsExactly("john@example.com", "jane@example.com");
        assertThat(EmailAddressUtils.normalizeAddresses("alpha; beta ; gamma"))
                .containsExactly("alpha", "beta", "gamma");
    }

    @Test
    void textAnalyticsUtilsHandlePreviewAndKeywords() {
        assertThat(TextAnalyticsUtils.toPreview(null, 10)).isEmpty();
        assertThat(TextAnalyticsUtils.toPreview("hello   world", 20)).isEqualTo("hello world");
        assertThat(TextAnalyticsUtils.toPreview("0123456789ABCDEF", 10)).isEqualTo("0123456...");
        assertThat(TextAnalyticsUtils.subjectKeywords("   ")).isEmpty();
        assertThat(TextAnalyticsUtils.subjectKeywords("-confirmed")).contains("confirmed");
        assertThat(TextAnalyticsUtils.subjectKeywords("re --  confirmed...   ")).contains("confirmed");
        assertThat(TextAnalyticsUtils.subjectKeywords("Re: Confirmed cases update for the region"))
                .contains("confirmed", "cases", "update", "region")
                .doesNotContain("re", "for", "the");
    }

    @Test
    void dateParsingUtilsHandleSupportedAndInvalidFormats() {
        assertThat(DateParsingUtils.parseEmailDate(null)).isEmpty();
        assertThat(DateParsingUtils.parseEmailDate("Mon, 14 May 2001 16:39:00 -0700 (PDT)"))
                .contains(Instant.parse("2001-05-14T23:39:00Z"));
        assertThat(DateParsingUtils.parseEmailDate("14 May 2001 16:39:00 +0000"))
                .contains(Instant.parse("2001-05-14T16:39:00Z"));
        assertThat(DateParsingUtils.parseEmailDate("14 May 2001 16:39:00"))
                .contains(Instant.parse("2001-05-14T16:39:00Z"));
        assertThat(DateParsingUtils.parseEmailDate("Mon, 14 May 2001 16:39:00"))
                .contains(Instant.parse("2001-05-14T16:39:00Z"));
        assertThat(DateParsingUtils.parseEmailDate("not a date")).isEmpty();
    }
}
