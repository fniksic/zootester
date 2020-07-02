package edu.upenn.zootester.harness;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class HarnessTest {

    private final Logger LOG = LoggerFactory.getLogger(HarnessTest.class);

    @Test
    public void testPossibleStates() {
        final Harness harness = new Harness(List.of(
                new UnconditionalWritePhase(2, "/key0", 102),
                new EmptyPhase(),
                new UnconditionalWritePhase(2, "/key1", 302)
        ));
        assertEquals(Set.of(Map.of("/key0", 102, "/key1", 302)), harness.getPossibleStates(Set.of(0, 2), Set.of()));
        assertEquals(Set.of(Map.of("/key0", 102, "/key1", 0)), harness.getPossibleStates(Set.of(0, 1), Set.of()));
        assertEquals(Set.of(Map.of("/key0", 0, "/key1", 302)), harness.getPossibleStates(Set.of(2), Set.of()));
        assertEquals(Set.of(Map.of("/key0", 0, "/key1", 0)), harness.getPossibleStates(Set.of(), Set.of()));

        assertEquals(Set.of(
                Map.of("/key0", 0, "/key1", 0),
                Map.of("/key0", 102, "/key1", 0)
        ), harness.getPossibleStates(Set.of(), Set.of(0)));

        assertEquals(Set.of(
                Map.of("/key0", 0, "/key1", 0),
                Map.of("/key0", 102, "/key1", 0)
        ), harness.getPossibleStates(Set.of(), Set.of(0, 1)));

        assertEquals(Set.of(
                Map.of("/key0", 0, "/key1", 0),
                Map.of("/key0", 102, "/key1", 0),
                Map.of("/key0", 0, "/key1", 302),
                Map.of("/key0", 102, "/key1", 302)
        ), harness.getPossibleStates(Set.of(), Set.of(0, 1, 2)));
    }

    @Test
    public void testPossibleStatesComplex_1() {
        final Harness harness = new Harness(List.of(
                new UnconditionalWritePhase(0, "/key0", 10),
                new UnconditionalWritePhase(1, "/key1", 11),
                new ConditionalWritePhase(0, "/key1", 11, "/key0", 20),
                new ConditionalWritePhase(1, "/key0", 10, "/key1", 21)
        ));
        final Set<Map<String, Integer>> expected = Set.of(
                Map.of("/key0", 10, "/key1", 21),
                Map.of("/key0", 20, "/key1", 11),
                Map.of("/key0", 20, "/key1", 21)
        );
        assertEquals(expected, harness.getPossibleStates(Set.of(0, 1, 2, 3), Set.of()));
    }

    @Test
    public void testPossibleStatesComplex_2() {
        final Harness harness = new Harness(List.of(
                new ConditionalWritePhase(2, "/key1", 0, "/key0", 102),
                new ConditionalWritePhase(1, "/key0", 0, "/key1", 201),
                new EmptyPhase()
        ));
        final Set<Map<String, Integer>> expected = Set.of(
                Map.of("/key0", 0, "/key1", 201),
                Map.of("/key0", 102, "/key1", 0),
                Map.of("/key0", 102, "/key1", 201)
        );
        assertEquals(expected, harness.getPossibleStates(Set.of(0, 1), Set.of()));
        assertEquals(Set.of(Map.of("/key0", 0, "/key1", 201)), harness.getPossibleStates(Set.of(1), Set.of()));
    }
}
