package edu.upenn.zktester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

@FunctionalInterface
public interface ZKProperty {

    boolean test(List<ZooKeeper> zookeepers) throws InterruptedException, KeeperException;

}
