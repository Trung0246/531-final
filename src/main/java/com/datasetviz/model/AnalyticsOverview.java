package com.datasetviz.model;

import java.time.Instant;

public class AnalyticsOverview {

    private int scannedFiles;
    private int parsedEmails;
    private int failedFiles;
    private int uniqueSenders;
    private int uniqueRecipients;
    private Instant firstEmailAt;
    private Instant lastEmailAt;

    public AnalyticsOverview() {
    }

    public AnalyticsOverview(int scannedFiles,
                             int parsedEmails,
                             int failedFiles,
                             int uniqueSenders,
                             int uniqueRecipients,
                             Instant firstEmailAt,
                             Instant lastEmailAt) {
        this.scannedFiles = scannedFiles;
        this.parsedEmails = parsedEmails;
        this.failedFiles = failedFiles;
        this.uniqueSenders = uniqueSenders;
        this.uniqueRecipients = uniqueRecipients;
        this.firstEmailAt = firstEmailAt;
        this.lastEmailAt = lastEmailAt;
    }

    public int getScannedFiles() {
        return scannedFiles;
    }

    public void setScannedFiles(int scannedFiles) {
        this.scannedFiles = scannedFiles;
    }

    public int getParsedEmails() {
        return parsedEmails;
    }

    public void setParsedEmails(int parsedEmails) {
        this.parsedEmails = parsedEmails;
    }

    public int getFailedFiles() {
        return failedFiles;
    }

    public void setFailedFiles(int failedFiles) {
        this.failedFiles = failedFiles;
    }

    public int getUniqueSenders() {
        return uniqueSenders;
    }

    public void setUniqueSenders(int uniqueSenders) {
        this.uniqueSenders = uniqueSenders;
    }

    public int getUniqueRecipients() {
        return uniqueRecipients;
    }

    public void setUniqueRecipients(int uniqueRecipients) {
        this.uniqueRecipients = uniqueRecipients;
    }

    public Instant getFirstEmailAt() {
        return firstEmailAt;
    }

    public void setFirstEmailAt(Instant firstEmailAt) {
        this.firstEmailAt = firstEmailAt;
    }

    public Instant getLastEmailAt() {
        return lastEmailAt;
    }

    public void setLastEmailAt(Instant lastEmailAt) {
        this.lastEmailAt = lastEmailAt;
    }
}
