package com.example.datasetviz.model;

public class TimeSeriesPoint {

    private String bucket;
    private long count;

    public TimeSeriesPoint() {
    }

    public TimeSeriesPoint(String bucket, long count) {
        this.bucket = bucket;
        this.count = count;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
