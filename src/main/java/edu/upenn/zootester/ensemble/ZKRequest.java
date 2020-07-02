package edu.upenn.zootester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

@FunctionalInterface
public interface ZKRequest {

    void apply(ZooKeeper zk, int serverId) throws KeeperException, InterruptedException;

}
