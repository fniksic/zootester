package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKRequest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Harness {

    private static final Logger LOG = LoggerFactory.getLogger(Harness.class);

    private final List<Phase> phases;
    private final List<String> keys;

    public Harness(final List<Phase> phases, final int numKeys) {
        this.phases = phases;
        this.keys = IntStream.range(0, numKeys)
                .mapToObj(Harness::keyMapper)
                .collect(Collectors.toList());
    }

    public ZKRequest getInitRequest() {
        return zk -> {
            final byte[] rawValue = "0".getBytes();
            final List<Op> ops = keys.stream()
                    .map(key -> Op.create(key, rawValue, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))
                    .collect(Collectors.toList());
            zk.multi(ops);
            keys.forEach(key -> LOG.info("Initial association: {} -> {}", key, 0));
        };
    }

    public List<Phase> getPhases() {
        return phases;
    }

    /**
     * {@code numPhases >= numRequests}
     *
     * @param numKeys
     * @param numNodes
     * @param numRequests
     * @param numPhases
     * @return
     */
    public static HarnessIterator generate(final int numKeys, final int numNodes, final int numRequests, final int numPhases) {
        return new HarnessIterator(numKeys, numNodes, numRequests, numPhases);
    }

    public static String keyMapper(final int key) {
        return "/key" + key;
    }
}
