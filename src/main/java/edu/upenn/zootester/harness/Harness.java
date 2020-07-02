package edu.upenn.zootester.harness;

import edu.upenn.zootester.ensemble.ZKProperty;
import edu.upenn.zootester.ensemble.ZKRequest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Harness {

    private static final Logger LOG = LoggerFactory.getLogger(Harness.class);

    private final List<Phase> phases;
    private final List<String> keys;

    private final Map<Set<Integer>, Set<Map<String, Integer>>> possibleStatesMap = new ConcurrentHashMap<>();

    public Harness(final List<Phase> phases, final int numKeys) {
        this.phases = phases;
        this.keys = IntStream.range(0, numKeys)
                .mapToObj(Harness::keyMapper)
                .collect(Collectors.toList());
    }

    public Harness(final List<Phase> phases) {
        this(phases, 2);
    }

    public ZKRequest getInitRequest() {
        return (zk, serverId) -> {
            final StringBuilder sb = new StringBuilder();
            sb.append("Initial request @ ").append(serverId).append(": Write");
            keys.forEach(key -> sb.append(' ').append(key).append(" -> ").append(0));
            LOG.info(sb.toString());
            final byte[] rawValue = "0".getBytes();
            final List<Op> ops = keys.stream()
                    .map(key -> Op.create(key, rawValue, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))
                    .collect(Collectors.toList());
            zk.multi(ops);
        };
    }

    public List<Phase> getPhases() {
        return phases;
    }

    public ZKProperty getConsistencyProperty(final Set<Integer> executedPhases,
                                             final Set<Integer> maybeExecutedPhases) {
        return new SequentialConsistency(keys, getPossibleStates(executedPhases, maybeExecutedPhases));
    }

    public Set<Map<String, Integer>> getPossibleStates(final Set<Integer> executedPhases,
                                                       final Set<Integer> maybeExecutedPhases) {
        final List<Integer> maybeList = new ArrayList<>(maybeExecutedPhases);
        final Set<Map<String, Integer>> result = new HashSet<>();

        // Note: This assumes there are at most 31 elements in maybeExecutedPhases.
        //       Either way, it won't work unless there are very few such elements.
        for (int subset = 0; subset < (1 << maybeList.size()); ++subset) {
            final Set<Integer> phases = new HashSet<>(executedPhases);
            for (int i = 0; i < maybeList.size(); ++i) {
                if ((subset & (1 << i)) != 0) {
                    phases.add(maybeList.get(i));
                }
            }
            if (!possibleStatesMap.containsKey(phases)) {
                computePossibleStates(phases);
            }
            result.addAll(possibleStatesMap.get(phases));
        }

        return result;
    }

    private Map<String, Integer> initialState() {
        return keys.stream().collect(Collectors.toMap(key -> key, key -> 0));
    }

    private void computePossibleStates(final Set<Integer> executedPhases) {
        final List<RequestPhase> expandedPhases = constructExpandedPhases(executedPhases);
        final List<Map<String, Integer>> states =
                new ArrayList<>(Collections.nCopies(expandedPhases.size() + 1, initialState()));
        final Set<Map<String, Integer>> possibleStates = new HashSet<>();
        recurse(new ArrayList<>(), new HashSet<>(), expandedPhases, states, 0, possibleStates);
        possibleStatesMap.put(executedPhases, possibleStates);
    }

    private void recurse(final List<Integer> positions,
                         final Set<Integer> setOfPositions,
                         final List<RequestPhase> expandedPhases,
                         final List<Map<String, Integer>> states,
                         final int from,
                         final Set<Map<String, Integer>> possibleStates) {
        if (positions.size() == expandedPhases.size()) {
            recomputeStates(positions, states, expandedPhases, from);
            possibleStates.add(states.get(states.size() - 1));
            return;
        }
        final Set<Integer> nodes = new HashSet<>();
        int nextFrom = from;
        for (int i = 0; i < expandedPhases.size(); ++i) {
            final RequestPhase phase = expandedPhases.get(i);
            if (setOfPositions.contains(i) || nodes.contains(phase.getNode())) {
                continue;
            }
            nodes.add(phase.getNode());
            positions.add(i);
            setOfPositions.add(i);

            recurse(positions, setOfPositions, expandedPhases, states, nextFrom, possibleStates);

            positions.remove(positions.size() - 1);
            setOfPositions.remove(i);
            nextFrom = positions.size();
        }
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

    private static void recomputeStates(final List<Integer> positions,
                                        final List<Map<String, Integer>> states,
                                        final List<RequestPhase> expandedPhases,
                                        int from) {
        for (final var i : positions.subList(from, positions.size())) {
            final Map<String, Integer> state = states.get(from);
            final Map<String, Integer> newState = new HashMap<>(state);
            final RequestPhase phase = expandedPhases.get(i);
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

    @Override
    public String toString() {
        return "Harness{" +
                "phases=" + phases +
                '}';
    }
}
