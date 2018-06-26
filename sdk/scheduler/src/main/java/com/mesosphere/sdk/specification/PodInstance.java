package com.mesosphere.sdk.specification;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A PodInstance defines a particular instance of a {@link PodSpec}. {@link PodSpec}s have an associated count, see
 * ({@link PodSpec#getCount()}).  When expanding that {@link PodSpec} to match the required count, {@link PodInstance}s
 * are generated.
 */
public class PodInstance {

    private final PodId podId;
    private final PodSpec podSpec;

    public PodInstance(PodSpec podSpec, int index) {
        this.podId = new PodId(podSpec, index);
        this.podSpec = podSpec;
    }

    /**
     * The distinct ID of this pod instance.
     */
    public PodId getId() {
        return podId;
    }

    /**
     * The specification that defines this pod instance.
     */
    public PodSpec getPod() {
        return podSpec;
    }

    @Override
    public String toString() {
        return podId.getName();
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
}
