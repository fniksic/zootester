package edu.upenn.zktester.harness;

import edu.upenn.zktester.ensemble.ZKProperty;
import edu.upenn.zktester.ensemble.ZKRequest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Harness {

    private static final Logger LOG = LoggerFactory.getLogger(Harness.class);

    private final List<Phase> phases;
    private final List<String> keys;

    private final Map<Set<Integer>, Set<Map<String, Integer>>> possibleStatesMap = new HashMap<>();

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

    public ZKProperty getConsistencyProperty(final Set<Integer> executedPhases) {
        return new SequentialConsistency(keys, getPossibleStates(executedPhases));
    }

    public Set<Map<String, Integer>> getPossibleStates(final Set<Integer> executedPhases) {
        if (!possibleStatesMap.containsKey(executedPhases)) {
            computePossibleStates(executedPhases);
        }
        return possibleStatesMap.get(executedPhases);
    }

    private static final Comparator<RequestPhase> INCREASING_NODES = Comparator.comparingInt(RequestPhase::getNode);

    private Map<String, Integer> initialState() {
        return keys.stream().collect(Collectors.toMap(key -> key, key -> 0));
    }

    private void computePossibleStates(final Set<Integer> executedPhases) {
        final List<RequestPhase> expandedPhases = constructExpandedPhases(executedPhases);
        final List<Map<String, Integer>> states =
                new ArrayList<>(Collections.nCopies(expandedPhases.size() + 1, initialState()));
        final Set<Map<String, Integer>> possibleStates = new HashSet<>();

        expandedPhases.sort(INCREASING_NODES);
        recomputeStates(states, expandedPhases, 0);
        possibleStates.add(states.get(states.size() - 1));

        while (true) {
            // Find a conflicting pair
            boolean found = false;
            int i = 0, j;
            for (j = expandedPhases.size() - 1; j > 0; --j) {
                final RequestPhase snd = expandedPhases.get(j);
                for (i = j - 1; i >= 0; --i) {
                    final RequestPhase fst = expandedPhases.get(i);
                    if (fst.getNode() == snd.getNode()) {
                        // We cannot swap phases with the same node
                        break;
                    }
                    final boolean inConflict = inConflict(fst, snd);
                    if (fst.getNode() > snd.getNode() && inConflict) {
                        // We don't want to re-explore reversals we've already explored.
                        break;
                    } else if (inConflict) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    break;
                }
            }
            if (!found) {
                // No more conflicts, we're done
                break;
            }

            // Reverse the order of the conflicted requests
            reversePair(expandedPhases, i, j);
            recomputeStates(states, expandedPhases, i);
            possibleStates.add(states.get(states.size() - 1));
        }
        possibleStatesMap.put(executedPhases, possibleStates);
    }

    private static boolean inConflict(final RequestPhase fst, final RequestPhase snd) {
        final String fstKey = fst.fullMatch(
                empty -> "",
                UnconditionalWritePhase::getWriteKey,
                ConditionalWritePhase::getReadKey,
                VirtualWritePhase::getWriteKey
        );
        final String sndKey = snd.fullMatch(
                empty -> "",
                UnconditionalWritePhase::getWriteKey,
                ConditionalWritePhase::getReadKey,
                VirtualWritePhase::getWriteKey
        );
        return (fst.isWrite() || snd.isWrite()) && fstKey.equals(sndKey);
    }

    private List<RequestPhase> constructExpandedPhases(final Set<Integer> executedPhases) {
        final List<RequestPhase> expandedPhases = new ArrayList<>();
        final SortedSet<Integer> sortedExecutedPhases = new TreeSet<>(executedPhases);
        for (final var i : sortedExecutedPhases) {
            final Phase phase = phases.get(i);
            phase.fullMatch(
                    emptyPhase -> null,
                    unconditionalWritePhase -> {
                        expandedPhases.add(unconditionalWritePhase);
                        return null;
                    },
                    conditionalWritePhase -> {
                        expandedPhases.add(conditionalWritePhase);
                        expandedPhases.add(new VirtualWritePhase(conditionalWritePhase));
                        return null;
                    },
                    virtualWritePhase -> null
            );
        }
        return expandedPhases;
    }

    private static void reversePair(final List<RequestPhase> expandedPhases, final int i, final int j) {
        final RequestPhase fst = expandedPhases.get(i);
        expandedPhases.set(i, expandedPhases.get(j));
        for (int k = j; k > i + 1; --k) {
            expandedPhases.set(k, expandedPhases.get(k - 1));
        }
        expandedPhases.set(i + 1, fst);
        if (i + 3 < expandedPhases.size()) {
            expandedPhases.subList(i + 2, expandedPhases.size()).sort(INCREASING_NODES);
        }
    }

    private static void recomputeStates(final List<Map<String, Integer>> states,
                                        final List<RequestPhase> expandedPhases,
                                        int from) {
        for (final var phase : expandedPhases.subList(from, expandedPhases.size())) {
            final Map<String, Integer> state = states.get(from);
            final Map<String, Integer> newState = new HashMap<>(state);
            phase.fullMatch(
                    empty -> null,
                    unconditionalWrite -> {
                        newState.put(unconditionalWrite.getWriteKey(), unconditionalWrite.getWriteValue());
                        return null;
                    },
                    conditionalWrite -> {
                        final boolean readSuccessful =
                                newState.get(conditionalWrite.getReadKey()) == conditionalWrite.getReadValue();
                        conditionalWrite.setReadSuccessful(readSuccessful);
                        return null;
                    },
                    virtualWrite -> {
                        if (virtualWrite.isWrite()) {
                            newState.put(virtualWrite.getWriteKey(), virtualWrite.getWriteValue());
                        }
                        return null;
                    }
            );
            ++from;
            states.set(from, newState);
        }
    }

    /**
     * {@code numPhases >= numRequests}
     *
     * @param numKeys     Number of keyes
     * @param numNodes    Number of nodes
     * @param numRequests Number of requests
     * @param numPhases   Number of phases
     * @return Iterator over an enumeration of harnesses that conform to the given parameter values
     */
    public static HarnessIterator generate(final int numKeys, final int numNodes, final int numRequests, final int numPhases) {
        return new HarnessIterator(numKeys, numNodes, numRequests, numPhases);
    }

    public static String keyMapper(final int key) {
        return "/key" + key;
    }
}
