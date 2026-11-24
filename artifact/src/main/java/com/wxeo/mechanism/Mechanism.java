package com.wxeo.mechanism;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * A ledger mechanism configuration (docs/method.md §4): the {@code process(B)} protocol with its
 * commit ordering, dedup and publication duties. One instance per service JVM.
 */
public interface Mechanism extends AutoCloseable {

    String id();

    /** Starts mechanism-owned background work (M2 relay). Called once before consuming. */
    default void init() throws Exception {
    }

    /** Called for every partition assignment (M4 seeks to its own stored progress here). */
    default void onAssignment(Collection<TopicPartition> assigned) throws Exception {
    }

    default void onRevoke(Collection<TopicPartition> revoked) {
    }

    /** Executes the three duties (effect, progress, publish) for one poll batch. */
    void process(Batch batch) throws Exception;

    /**
     * Recovery hook after an injected transaction abort / generic injected fault: the mechanism has
     * already rolled back its open transaction in {@link #process}'s error path; a mechanism may
     * additionally reset its own state here.
     */
    default void recoverAfterInjectedFault() {
    }

    @Override
    default void close() throws Exception {
    }
}
