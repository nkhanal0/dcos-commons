package com.mesosphere.sdk.scheduler.plan;

import com.google.common.annotations.VisibleForTesting;

import com.mesosphere.sdk.offer.OfferRecommendation;
import com.mesosphere.sdk.specification.PodInstance;

import org.apache.mesos.Protos;

import java.util.*;

/**
 * This class is an implementation of the Step interface for test purposes.
 */
public class TestStep extends AbstractStep {

    private final PodInstance podInstance;
    private final PodLaunch podLaunch;

    public TestStep() {
        super("test-step", Optional.empty());
        this.podInstance = null;
        this.podLaunch = null;
    }

    public TestStep(String name, PodInstance podInstance, PodLaunch podLaunch) {
        super(name, Optional.empty());
        this.podInstance = podInstance;
        this.podLaunch = podLaunch;
    }

    public TestStep(UUID id, String name, PodInstance podInstance, PodLaunch podLaunch) {
        super(name, Optional.empty());
        this.id = id;
        this.podInstance = podInstance;
        this.podLaunch = podLaunch;
    }

    @Override
    public void start() {
        setStatus(Status.PREPARED);
    }

    @Override
    public Optional<PodInstance> getPodInstance() {
        return Optional.ofNullable(podInstance);
    }

    @Override
    public Optional<PodLaunch> getPodLaunch() {
        return Optional.ofNullable(podLaunch);
    }

    @Override
    public void updateOfferStatus(Collection<OfferRecommendation> recommendations) {
        if (recommendations.isEmpty()) {
            setStatus(Status.PREPARED);
        } else {
            setStatus(Status.STARTING);
        }
    }

    @Override
    public void update(Protos.TaskStatus status) {
        // Left intentionally empty
    }

    @Override
    public String getDisplayStatus() {
        return getStatus().toString();
    }

    @Override
    public List<String> getErrors() {
        return Collections.emptyList();
    }

    @Override
    public void restart() {
        setStatus(Status.PENDING);
    }

    @Override
    public void forceComplete() {
        setStatus(Status.COMPLETE);
    }

    @VisibleForTesting
    public void setStatus(Status status) {
        super.setStatus(status);
    }

    @Override
    public String toString() {
        return String.format("TestStep[%s:%s]", getName(), getStatus());
    }
}
