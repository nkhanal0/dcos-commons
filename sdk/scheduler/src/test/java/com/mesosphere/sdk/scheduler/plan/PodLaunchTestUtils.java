package com.mesosphere.sdk.scheduler.plan;

import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.specification.*;
import com.mesosphere.sdk.testutils.TestConstants;
import org.apache.mesos.Protos;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Utilities relating to constructing {@link PodLaunch}s.
 */
public class PodLaunchTestUtils {

    private PodLaunchTestUtils() {
        // do not instantiate
    }

    public static PodLaunch getCpuRequirement(double value) {
        return getCpuRequirement(value, 0);
    }

    public static PodLaunch getCpuRequirement(double value, String preReservedRole) {
        return getRequirement(getCpuResourceSet(value, preReservedRole), 0);
    }

    public static PodLaunch getCpuRequirement(double value, int index) {
        return getRequirement(getCpuResourceSet(value), index);
    }

    public static PodLaunch getRootVolumeRequirement(double cpus, double diskSize) {
        return getRootVolumeRequirement(cpus, diskSize, 0);
    }

    public static PodLaunch getMountVolumeRequirement(double cpus, double diskSize) {
        return getMountVolumeRequirement(cpus, diskSize, 0);
    }

    public static PodLaunch getRootVolumeRequirement(double cpus, double diskSize, int index) {
        return getRequirement(getRootVolumeResourceSet(cpus, diskSize), index);
    }

    public static PodLaunch getMountVolumeRequirement(double cpus, double diskSize, int index) {
        return getRequirement(getMountVolumeResourceSet(cpus, diskSize), index);
    }

    public static PodLaunch getPortRequirement(int... ports) {
        Map<String, Integer> envPorts = new HashMap<>();
        for (int i = 0; i < ports.length; ++i) {
            // Default env: "TEST_PORT_NAME_<portnum>"
            envPorts.put(TestConstants.PORT_ENV_NAME + "_" + ports[i], ports[i]);
        }
        return getPortRequirement(envPorts);
    }

    public static PodLaunch getPortRequirement(Map<String, Integer> envPorts) {
        return getRequirement(getPortsResourceSet(envPorts), 0);
    }

    public static PodLaunch getVIPRequirement(int vipPort, int taskPort) {
        return getRequirement(getVIPResourceSet(Collections.singletonMap(vipPort, taskPort)), 0);
    }

    public static ResourceSet getCpuResourceSet(double value) {
        return getCpuResourceSet(value, Constants.ANY_ROLE);
    }

    public static ResourceSet getCpuResourceSet(double value, String preReservedRole) {
        return DefaultResourceSet.newBuilder(TestConstants.ROLE, preReservedRole, TestConstants.PRINCIPAL)
                .id(TestConstants.RESOURCE_SET_ID)
                .cpus(value)
                .build();
    }

    /**
     * Gets a test root volume resource set
     * @param cpus Some resource other than disk must be specified so CPU size is required.
     * @param diskSize The disk size required.
     */
    private static ResourceSet getRootVolumeResourceSet(double cpus, double diskSize) {
        return getVolumeResourceSet(cpus, diskSize, VolumeSpec.Type.ROOT.name());
    }

    private static ResourceSet getMountVolumeResourceSet(double cpus, double diskSize) {
        return getVolumeResourceSet(cpus, diskSize, VolumeSpec.Type.MOUNT.name());
    }

    private static ResourceSet getVolumeResourceSet(double cpus, double diskSize, String diskType) {
        return DefaultResourceSet.newBuilder(TestConstants.ROLE, Constants.ANY_ROLE, TestConstants.PRINCIPAL)
                .id(TestConstants.RESOURCE_SET_ID)
                .cpus(cpus)
                .addVolume(diskType, diskSize, TestConstants.CONTAINER_PATH)
                .build();
    }

    private static ResourceSet getPortsResourceSet(Map<String, Integer> envPorts) {
        DefaultResourceSet.Builder builder =
                DefaultResourceSet.newBuilder(TestConstants.ROLE, Constants.ANY_ROLE, TestConstants.PRINCIPAL)
                .id(TestConstants.RESOURCE_SET_ID);
        for (Map.Entry<String, Integer> envPort : envPorts.entrySet()) {
            Protos.Value.Builder valueBuilder = Protos.Value.newBuilder()
                    .setType(Protos.Value.Type.RANGES);
            valueBuilder.getRangesBuilder().addRangeBuilder()
                    .setBegin(envPort.getValue())
                    .setEnd(envPort.getValue());
            builder.addResource(new PortSpec(
                    valueBuilder.build(),
                    TestConstants.ROLE,
                    Constants.ANY_ROLE,
                    TestConstants.PRINCIPAL,
                    envPort.getKey(),
                    String.format("test-port-%s", envPort.getKey()),
                    TestConstants.PORT_VISIBILITY,
                    Collections.emptyList()));
        }
        return builder.build();
    }

    private static ResourceSet getVIPResourceSet(Map<Integer, Integer> vipPorts) {
        DefaultResourceSet.Builder builder =
                DefaultResourceSet.newBuilder(TestConstants.ROLE, Constants.ANY_ROLE, TestConstants.PRINCIPAL)
                .id(TestConstants.RESOURCE_SET_ID);
        for (Map.Entry<Integer, Integer> entry : vipPorts.entrySet()) {
            int taskPort = entry.getValue();
            Protos.Value.Builder valueBuilder = Protos.Value.newBuilder()
                    .setType(Protos.Value.Type.RANGES);
            valueBuilder.getRangesBuilder().addRangeBuilder()
                    .setBegin(taskPort)
                    .setEnd(taskPort);
            builder.addResource(new NamedVIPSpec(
                    valueBuilder.build(),
                    TestConstants.ROLE,
                    Constants.ANY_ROLE,
                    TestConstants.PRINCIPAL,
                    TestConstants.PORT_ENV_NAME + "_VIP_" + taskPort,
                    TestConstants.VIP_NAME + "-" + taskPort,
                    "tcp",
                    TestConstants.PORT_VISIBILITY,
                    TestConstants.VIP_NAME + "-" + taskPort,
                    entry.getKey(),
                    Collections.emptyList()));
        }
        return builder.build();

    }

    public static PodLaunch getRequirement(ResourceSet resourceSet, int index) {
        return getRequirement(resourceSet, new PodId(TestConstants.POD_TYPE, index));
    }

    public static PodLaunch getRequirement(ResourceSet resourceSet, PodId podId) {
        TaskSpec taskSpec = DefaultTaskSpec.newBuilder()
                .name(TestConstants.TASK_NAME)
                .commandSpec(
                        DefaultCommandSpec.newBuilder(Collections.emptyMap())
                                .value(TestConstants.TASK_CMD)
                                .build())
                .goalState(GoalState.RUNNING)
                .resourceSet(resourceSet)
                .build();

        PodSpec podSpec = DefaultPodSpec.newBuilder(podId.getType(), 1, Arrays.asList(taskSpec))
                .preReservedRole(Constants.ANY_ROLE)
                .build();

        PodInstance podInstance = new PodInstance(podSpec, podId.getIndex());

        List<String> taskNames = podInstance.getPod().getTasks().stream()
                .map(ts -> ts.getName())
                .collect(Collectors.toList());

        return PodLaunch.newBuilder(podInstance, taskNames).build();
    }

    public static PodLaunch getExecutorRequirement(
            ResourceSet taskResources,
            Collection<VolumeSpec> executorVolumes,
            String type,
            int index) {
        TaskSpec taskSpec = DefaultTaskSpec.newBuilder()
                .name(TestConstants.TASK_NAME)
                .commandSpec(
                        DefaultCommandSpec.newBuilder(Collections.emptyMap())
                                .value(TestConstants.TASK_CMD)
                                .build())
                .goalState(GoalState.RUNNING)
                .resourceSet(taskResources)
                .build();

        PodSpec podSpec = DefaultPodSpec.newBuilder(type, 1, Arrays.asList(taskSpec))
                .volumes(executorVolumes)
                .build();

        PodInstance podInstance = new PodInstance(podSpec, index);

        List<String> taskNames = podInstance.getPod().getTasks().stream()
                .map(ts -> ts.getName())
                .collect(Collectors.toList());

        return PodLaunch.newBuilder(podInstance, taskNames).build();
    }
}
