package com.datasetviz.model;

public class CommunicationEdge {

    private String source;
    private String target;
    private long count;

    public CommunicationEdge() {
    }

    public CommunicationEdge(String source, String target, long count) {
        this.source = source;
        this.target = target;
        this.count = count;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
