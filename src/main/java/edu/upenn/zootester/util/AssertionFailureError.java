package edu.upenn.zootester.util;

public class AssertionFailureError extends Error {

    public AssertionFailureError(final String message) {
        super(message);
    }
}
