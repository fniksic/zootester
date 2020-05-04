package edu.upenn.zktester.util;

import org.apache.zookeeper.KeeperException;

@FunctionalInterface
public interface ThrowingFunction<T, R> {

    R apply(T t) throws InterruptedException, KeeperException;

}