package edu.upenn.zktester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

@FunctionalInterface
public interface ZKRequest {

    void apply(ZooKeeper zk) throws KeeperException, InterruptedException;

}
