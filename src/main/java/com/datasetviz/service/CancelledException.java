package com.datasetviz.service;

public class CancelledException extends RuntimeException {

    public CancelledException(String message) {
        super(message);
    }
}
