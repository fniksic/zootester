package edu.upenn.zootester.util;

public class Assert {
    public static void fail(final String message) {
        throw new AssertionFailureError(message);
    }

    public static void assertTrue(final String message, final boolean condition) {
        if (!condition) {
            fail(message);
        }
    }
}
