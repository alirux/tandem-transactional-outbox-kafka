package com.codingful.tandem.jdbc;

/**
 * The {@link WorkerPool}'s side of a {@link WakeupSource} (dispatch-latency §6): it turns a bucket
 * number into a wait that ends early, for the one worker owning that bucket's slice.
 *
 * <p>Each worker waits on a slot of its own rather than on a shared condition, so a signal wakes the
 * one thread that can act on it and leaves the rest asleep — the property that keeps a wakeup's cost
 * independent of the worker count. A signal <b>is remembered</b> until it is consumed: it may well
 * arrive while its worker is mid-claim rather than waiting, and a signal dropped there would leave the
 * row it announces waiting a full poll interval, which is exactly the case the mechanism exists for.
 *
 * <p>Extra wakes are harmless by construction — a woken worker that finds nothing to claim simply
 * waits again — so the slot is a single flag rather than a count: what matters is <i>that</i>
 * something was written, never how many times.
 */
final class WorkerWakeups implements WakeupSource.Listener {

    private final Slot[] slots;

    WorkerWakeups(int workerCount) {
        this.slots = new Slot[workerCount];
        for (int i = 0; i < workerCount; i++) {
            slots[i] = new Slot();
        }
    }

    /** Wakes the worker owning {@code bucket}, under the same {@code bucket % workerCount} split the pool uses. */
    @Override
    public void wake(int bucket) {
        slots[Math.floorMod(bucket, slots.length)].signal();
    }

    @Override
    public void wakeAll() {
        for (Slot slot : slots) {
            slot.signal();
        }
    }

    /**
     * Waits out this worker's idle backoff, returning as soon as a signal for its slice arrives.
     *
     * @param index   the worker's index in the pool
     * @param millis  the backoff to wait at most, always positive
     * @return {@code true} if a signal ended the wait, {@code false} if the backoff simply elapsed
     * @throws InterruptedException if the worker thread is interrupted, as a shutdown does
     */
    boolean await(int index, long millis) throws InterruptedException {
        return slots[index].await(millis);
    }

    /**
     * One worker's pending-signal flag and the monitor its thread waits on. A plain
     * {@code synchronized} pair rather than a lock or a semaphore: the flag is set at most once per
     * wait and read by exactly one thread, and a semaphore's permits would accumulate under a live
     * stream into as many immediate wake-ups as there were writes.
     */
    private static final class Slot {

        private boolean signalled;

        synchronized void signal() {
            signalled = true;
            notifyAll();
        }

        synchronized boolean await(long millis) throws InterruptedException {
            if (!signalled) {
                // Waited once, not until signalled: this is a bounded wait by contract, so a spurious
                // return is simply an early poll — which is the loop's normal behaviour anyway.
                wait(millis);
            }
            boolean woken = signalled;
            signalled = false;
            return woken;
        }
    }
}
