package edu.upenn.zootester.util;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

@FunctionalInterface
public interface ThrowingFunction<T, R> {

    R apply(T t) throws InterruptedException, KeeperException, IOException;

}
