package com.mesosphere.sdk.specification;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A unique identifier for pod instances. Consists of a pod type and a pod index.
 */
public class PodId {
    private final String type;
    private final int index;

    public PodId(String type, int index) {
        this.type = type;
        this.index = index;
    }

    public PodId(PodSpec podSpec, int index) {
        this(podSpec.getType(), index);
    }

    /**
     * The type of this pod instance. This is the label chosen by the developer when defining the service spec.
     */
    public String getType() {
        return type;
    }

    /**
     * The index of this pod instance. Each pod instance has a unique index, starting at zero.
     */
    public int getIndex() {
        return index;
    }

    /**
     * Returns a descriptive name for the pod, of the form {@code <type>-<index>}.
     */
    public String getName() {
        return getName(type, index);
    }

    /**
     * Returns a descriptive name for the pod, of the form {@code <type>-<index>}.
     */
    public static String getName(String podType, int index) {
        return String.format("%s-%d", podType, index);
    }

    @Override
    public String toString() {
        return getName();
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
