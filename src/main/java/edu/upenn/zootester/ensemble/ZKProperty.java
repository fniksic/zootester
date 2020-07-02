package edu.upenn.zootester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

@FunctionalInterface
public interface ZKProperty {

    boolean test(List<ZooKeeper> clients, List<Integer> clientForServer) throws InterruptedException, KeeperException;

}
