package com.mesosphere.sdk.scheduler.plan;

import com.mesosphere.sdk.scheduler.recovery.RecoveryType;
import com.mesosphere.sdk.specification.PodId;
import com.mesosphere.sdk.specification.PodInstance;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link PodLaunch} describes information relevant to launching tasks within a pod.
 */
public class PodLaunch {

    private final PodId podId;
    private final Collection<String> tasksToLaunch;
    private final Map<String, String> environment;
    private final RecoveryType recoveryType;

    public static Builder newBuilder(PodInstance podInstance, Collection<String> tasksToLaunch) {
        return new Builder(podInstance.getId(), tasksToLaunch);
    }

    public static Builder newBuilder(PodLaunch podLaunch) {
        return new Builder(podLaunch.podId, podLaunch.tasksToLaunch);
    }

    /**
     * Creates a new instance with the provided permanent replacement setting.
     */
    private PodLaunch(Builder builder) {
        this.podId = builder.podId;
        this.tasksToLaunch = builder.tasksToLaunch;
        this.environment = builder.environment;
        this.recoveryType = builder.recoveryType;
    }

    /**
     * Returns the ID of the pod to be launched.
     */
    public PodId getId() {
        return podId;
    }

    /**
     * Returns the list of tasks to be launched within this pod. This doesn't necessarily match the tasks listed in the
     * {@link PodInstance}.
     */
    public Collection<String> getTasksToLaunch() {
        return tasksToLaunch;
    }

    /**
     * Returns the map of environment variable names to values that extend the existing environments of tasks in this
     * pod.
     */
    public Map<String, String> getEnvironment() {
        return environment;
    }

    /**
     * Returns the recovery type being exercised in this launch operation, or {@code NONE} if no recovery is being
     * performed.
     */
    public RecoveryType getRecoveryType() {
        return recoveryType;
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", podId.getName(), recoveryType);
    }

    /**
     * A PodInstanceRequirement conflictsWith with another it if applies to the same pod instance and some
     * tasks in that pod.
     *
     * pod-0:[task0, task1]          conflictsWith with pod-0:[task1]
     * pod-0:[task1]        does NOT conflict  with pod-0:[task0]
     * pod-0:[task0]        does NOT conflict  with pod-1:[task0]
     */
    public boolean conflictsWith(PodLaunch podLaunch) {
        return podId.equals(podLaunch.podId)
                && getTasksToLaunch().stream()
                        .filter(t -> podLaunch.getTasksToLaunch().contains(t))
                        .count() > 0;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    /**
     * {@link PodLaunch} builder static inner class.
     */
    public static final class Builder {
        private final PodId podId;
        private final Collection<String> tasksToLaunch;

        private Map<String, String> environment = new HashMap<>();
        private RecoveryType recoveryType = RecoveryType.NONE;

        private Builder(PodId podId, Collection<String> tasksToLaunch) {
            this.podId = podId;
            this.tasksToLaunch = tasksToLaunch;
        }

        public Builder environment(Map<String, String> environment) {
            this.environment = environment;
            return this;
        }

        public Builder recoveryType(RecoveryType recoveryType) {
            this.recoveryType = recoveryType;
            return this;
        }

        public PodLaunch build() {
            return new PodLaunch(this);
        }
    }
}
