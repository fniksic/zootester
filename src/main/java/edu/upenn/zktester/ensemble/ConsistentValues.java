package edu.upenn.zktester.ensemble;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ConsistentValues implements ZKProperty {

    private static final Logger LOG = LoggerFactory.getLogger(ConsistentValues.class);

    private final List<String> keys;

    public ConsistentValues(final List<String> keys) {
        this.keys = keys;
    }

    @Override
    public boolean test(List<ZooKeeper> zookeepers) throws InterruptedException, KeeperException {
        boolean result = true;
        for (final var key : keys) {
            final ZooKeeper zk0 = zookeepers.get(0);
            final byte[] rawValue0 = zk0.getData(key, false, null);
            LOG.info("{}\n\tAssociation: {} -> {}", zk0.toString(), key, new String(rawValue0));

            final boolean valueOK = zookeepers.subList(1, zookeepers.size()).stream()
                    .allMatch(zk -> {
                        try {
                            final byte[] rawValue = zk.getData(key, false, null);
                            LOG.info("Association: {} -> {}", key, new String(rawValue));
                            return Arrays.equals(rawValue0, rawValue);
                        } catch (final KeeperException | InterruptedException e) {
                            return false;
                        }
                    });
            result = result && valueOK;
        }
        return result;
    }
}
