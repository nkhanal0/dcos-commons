package com.mesosphere.sdk.scheduler.recovery;

import com.mesosphere.sdk.config.SerializationUtils;
import com.mesosphere.sdk.http.types.PlanInfo;
import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.offer.TaskUtils;
import com.mesosphere.sdk.scheduler.plan.*;
import com.mesosphere.sdk.scheduler.plan.strategy.ParallelStrategy;
import com.mesosphere.sdk.scheduler.recovery.constrain.LaunchConstrainer;
import com.mesosphere.sdk.scheduler.recovery.monitor.FailureMonitor;
import com.mesosphere.sdk.specification.PodInstance;
import com.mesosphere.sdk.specification.PodSpec;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.specification.TaskSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.state.StateStoreUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link RecoveryPlanManager} enables monitoring and management of recovery plan.
 * <p>
 * This is an implementation of {@code PlanManager} that performs task recovery using dynamically generated
 * {@code Plan}. {@link RecoveryPlanManager} tracks currently failed (permanent) and stopped (transient) tasks,
 * generates a new {@link RecoveryStep} for them and adds them to the recovery Plan, if not already added.
 */
public class RecoveryPlanManager implements PlanManager {

    protected final Logger logger;
    protected final ConfigStore<ServiceSpec> configStore;

    private final Optional<String> namespace;
    private final List<RecoveryPlanOverrider> recoveryPlanOverriders;
    private final Set<String> recoverableTaskNames;

    protected volatile Plan plan;

    protected final ServiceSpec serviceSpec;
    protected final StateStore stateStore;
    protected final FailureMonitor failureMonitor;
    protected final LaunchConstrainer launchConstrainer;
    protected final Object planLock = new Object();

    public RecoveryPlanManager(
            ServiceSpec serviceSpec,
            StateStore stateStore,
            ConfigStore<ServiceSpec> configStore,
            Set<String> recoverableTaskNames,
            LaunchConstrainer launchConstrainer,
            FailureMonitor failureMonitor,
            Optional<String> namespace,
            List<RecoveryPlanOverrider> recoveryPlanOverriders) {
        this.logger = LoggingUtils.getLogger(getClass(), namespace);
        this.serviceSpec = serviceSpec;
        this.stateStore = stateStore;
        this.configStore = configStore;
        this.recoverableTaskNames = recoverableTaskNames;
        this.failureMonitor = failureMonitor;
        this.launchConstrainer = launchConstrainer;
        this.namespace = namespace;
        this.recoveryPlanOverriders = recoveryPlanOverriders;
        plan = new DefaultPlan(Constants.RECOVERY_PLAN_NAME, Collections.emptyList());
    }

    @Override
    public Plan getPlan() {
        synchronized (planLock) {
            return plan;
        }
    }

    @Override
    public void setPlan(Plan plan) {
        throw new UnsupportedOperationException("Setting plans on the RecoveryPlanManager is not allowed.");
    }

    protected void setPlanInternal(Plan plan) {
        synchronized (planLock) {
            this.plan = plan;
            if (!plan.getChildren().isEmpty()) {
                try {
                    logger.info("Recovery plan set to: {}",
                            SerializationUtils.toShortJsonString(PlanInfo.forPlan(plan)));
                } catch (IOException e) {
                    List<String> stepNames = plan.getChildren().stream()
                            .flatMap(phase -> phase.getChildren().stream())
                            .map(step -> step.getName())
                            .collect(Collectors.toList());
                    logger.error("Failed to serialize plan to JSON. Recovery plan set to: {}", stepNames);
                }
            }
        }
    }

    @Override
    public Collection<? extends Step> getCandidates(Collection<PodLaunch> dirtyAssets) {
        synchronized (planLock) {
            updatePlan(dirtyAssets);
            return getPlan().getCandidates(dirtyAssets).stream()
                    .filter(step ->
                            launchConstrainer.canLaunch(((RecoveryStep) step).getRecoveryType()))
                    .collect(Collectors.toList());
        }
    }


    /**
     * Updates the recovery plan if necessary.
     * <p>
     * 1. Updates existing steps.
     * 2. If the needs recovery and doesn't yet have a step in the plan, removes any COMPLETED steps for this task
     * (at most one step for a given task can exist) and creates a new PENDING step.
     *
     * @param status task status
     */
    @Override
    public void update(Protos.TaskStatus status) {
        synchronized (planLock) {
            getPlan().update(status);
        }
    }

    protected void updatePlan(Collection<PodLaunch> dirtyAssets) {
        if (!dirtyAssets.isEmpty()) {
            logger.info("Dirty assets for recovery plan consideration: {}", dirtyAssets);
        }

        synchronized (planLock) {
            Collection<PodLaunch> newPodLaunches = null;

            try {
                newPodLaunches = getNewRecoveryRequirements(dirtyAssets);
            } catch (TaskException e) {
                logger.error("Failed to generate steps.", e);
                return;
            }

            List<PodLaunch> podLaunches = new ArrayList<>();
            List<Phase> phases = new ArrayList<>();
            for (PodLaunch podLaunch : newPodLaunches) {
                boolean overridden = false;
                for (RecoveryPlanOverrider overrider : recoveryPlanOverriders) {
                    Optional<Phase> override  = overrider.override(podLaunch);
                    if (override.isPresent()) {
                        overridden = true;
                        phases.add(override.get());
                    }
                }

                if (!overridden) {
                    podLaunches.add(podLaunch);
                }
            }

            phases.addAll(podLaunches.stream()
                    .map(podLaunch -> createStep(podLaunch))
                    .map(step -> new DefaultPhase(
                            step.getName(),
                            Arrays.asList(step),
                            new ParallelStrategy<Step>(),
                            Collections.emptyList()))
                    .collect(Collectors.toList()));
            setPlanInternal(updatePhases(phases));
        }
    }

    private static boolean phaseOverriden(Phase phase, Collection<PodLaunch> newReqs) {
        // If at least 1 new requirement conflicts with the phase's steps, the phase has been overridden
        return phase.getChildren().stream()
                .map(step -> step.getPodLaunch())
                .anyMatch(launch -> launch.isPresent() && PlanUtils.assetConflicts(launch.get(), newReqs));
    }

    private Plan updatePhases(List<Phase> overridePhases) {
        Collection<PodLaunch> newReqs = overridePhases.stream()
                .flatMap(phase -> phase.getChildren().stream())
                .map(step -> step.getPodLaunch())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        Collection<Phase> inProgressPhases = getPlan().getChildren().stream()
                .filter(phase -> !phaseOverriden(phase, newReqs))
                .collect(Collectors.toList());

        List<Phase> phases = new ArrayList<>();
        phases.addAll(inProgressPhases);
        phases.addAll(overridePhases);

        return DeployPlanFactory.getPlan(Constants.RECOVERY_PLAN_NAME, phases, new ParallelStrategy<>());
    }

    private boolean isTaskPermanentlyFailed(Protos.TaskInfo taskInfo) {
        return FailureUtils.isPermanentlyFailed(taskInfo) || failureMonitor.hasFailed(taskInfo);
    }

    private List<PodLaunch> getNewRecoveryRequirements(Collection<PodLaunch> dirtyAssets)
            throws TaskException {

        List<PodLaunch> newFailedPods = getNewFailedPods(dirtyAssets);
        List<PodLaunch> recoveryRequirements = new ArrayList<>();

        for (PodLaunch failedPod : newFailedPods) {
            List<Protos.TaskInfo> failedPodTaskInfos = failedPod.getTasksToLaunch().stream()
                    .map(taskSpecName -> TaskSpec.getInstanceName(failedPod.getId(), taskSpecName))
                    .map(taskInfoName -> stateStore.fetchTask(taskInfoName))
                    .filter(taskInfo -> taskInfo.isPresent())
                    .map(taskInfo -> taskInfo.get())
                    .collect(Collectors.toList());

            logFailedPod(failedPod.getId().getName(), failedPodTaskInfos);

            RecoveryType recoveryType = getTaskRecoveryType(failedPodTaskInfos);
            if (RecoveryType.NONE.equals(recoveryType)) {
                logger.error(
                        "Cannot recover tasks within pod: '{}' due to having recovery type: '{}'.",
                        failedPod.getId().getName(),
                        recoveryType.name());
                continue;
            }

            logger.info("Recovering {} failed pod: '{}'", recoveryType.name(), failedPod);
            PodLaunch podLaunch = PodLaunch.newBuilder(failedPod)
                    .recoveryType(recoveryType)
                    .build();

            if (PlanUtils.assetConflicts(podLaunch, dirtyAssets)) {
                logger.info("Pod: {} has been dirtied by another plan, cannot recover at this time.", failedPod);
            } else {
                recoveryRequirements.add(podLaunch);
            }
        }

        return recoveryRequirements;
    }

    private List<PodLaunch> getNewFailedPods(Collection<PodLaunch> dirtyAssets)
            throws TaskException {
        Collection<Protos.TaskInfo> failedTasks =
                StateStoreUtils.fetchTasksNeedingRecovery(stateStore, configStore, recoverableTaskNames);
        if (!failedTasks.isEmpty()) {
            logger.info("Tasks needing recovery: {}", getTaskNames(failedTasks));
        }

        List<PodLaunch> failedPods = TaskUtils.getPodRequirements(stateStore, configStore, failedTasks);
        if (!failedPods.isEmpty()) {
            logger.info("All failed pods: {}", getPodNames(failedPods));
        }

        failedPods = failedPods.stream()
                .filter(pod -> !PlanUtils.assetConflicts(pod, dirtyAssets))
                .collect(Collectors.toList());
        if (!failedPods.isEmpty()) {
            logger.info("Pods needing recovery: {}", getPodNames(failedPods));
        }

        List<Step> incompleteSteps = getPlan().getChildren().stream()
                .flatMap(phase -> phase.getChildren().stream())
                .filter(step ->
                        !step.isComplete()
                        && step.getPodInstance().isPresent()
                        && step.getPodLaunch().isPresent())
                .collect(Collectors.toList());
        if (!incompleteSteps.isEmpty()) {
            logger.info("Pods with incomplete launches: {}",
                    getPodNames(incompleteSteps.stream()
                            .map(step -> step.getPodLaunch().get())
                            .collect(Collectors.toList())));
        }

        // Build map of Steps (PodInstanceRequirements) to pod instances
        // e.g. journal-0 --> [bootstrap, node]

        // Initialize map with empty arrays for values
        Map<PodInstance, List<PodLaunch>> recoveryMap = new HashMap<>();
        for (Step incompleteStep : incompleteSteps) {
            List<PodLaunch> podLaunches = recoveryMap.get(incompleteStep.getPodInstance().get());
            if (podLaunches == null) {
                podLaunches = new ArrayList<>();
                recoveryMap.put(incompleteStep.getPodInstance().get(), podLaunches);
            }
            podLaunches.add(incompleteStep.getPodLaunch().get());
        }

        Collection<PodLaunch> inProgressRecoveries = new ArrayList<>();
        for (Map.Entry<PodInstance, List<PodLaunch>> entry : recoveryMap.entrySet()) {
            // If a pod's failure state has escalated (transient --> permanent) it should NOT be counted as
            // in progress, so that it can be re-evaluated the for the appropriate recovery approach.
            if (!failureStateHasEscalated(entry.getKey(), entry.getValue())) {
                inProgressRecoveries.addAll(entry.getValue());
            }
        }
        if (!inProgressRecoveries.isEmpty()) {
            logger.info("Pods with recoveries already in progress: {}", getPodNames(inProgressRecoveries));
        }

        failedPods = failedPods.stream()
                .filter(pod -> !PlanUtils.assetConflicts(pod, inProgressRecoveries))
                .collect(Collectors.toList());
        if (!failedPods.isEmpty()) {
            logger.info("New pods needing recovery: {}", getPodNames(failedPods));
        }

        return failedPods;
    }

    private void logFailedPod(String failedPodName, List<Protos.TaskInfo> failedTasks) {
        List<String> permanentlyFailedTasks = failedTasks.stream()
                .filter(taskInfo -> isTaskPermanentlyFailed(taskInfo))
                .map(taskInfo -> taskInfo.getName())
                .collect(Collectors.toList());

        List<String> transientlyFailedTasks = failedTasks.stream()
                .filter(taskInfo -> !isTaskPermanentlyFailed(taskInfo))
                .map(taskInfo -> taskInfo.getName())
                .collect(Collectors.toList());

        if (!permanentlyFailedTasks.isEmpty() || !transientlyFailedTasks.isEmpty()) {
            logger.info("Failed tasks in pod: {}, permanent{}, transient{}",
                    failedPodName, permanentlyFailedTasks, transientlyFailedTasks);
        }
    }

    /**
     * A pod instance failure has escalated if it has transitioned from TRANSIENT to PERMANENT ({@link RecoveryType}.
     */
    private boolean failureStateHasEscalated(PodInstance podInstance, Collection<PodLaunch> podInstanceRequirements) {

        Collection<Protos.TaskInfo> taskInfos =
                StateStoreUtils.fetchPodTasks(stateStore, podInstance).stream().collect(Collectors.toList());

        RecoveryType original = getPodRecoveryType(podInstanceRequirements);
        RecoveryType current = getTaskRecoveryType(taskInfos);

        boolean failureStateHasEscalated = false;
        if (original.equals(RecoveryType.TRANSIENT) && current.equals(RecoveryType.PERMANENT)) {
            failureStateHasEscalated = true;
        }

        String mode = failureStateHasEscalated ? "" : "NOT ";
        List<String> podNames = getPodNames(podInstanceRequirements);
        logger.info("Failure state for {} has {}escalated. original: {}, current: {}",
                podNames, mode, original, current);

        return failureStateHasEscalated;
    }

    private RecoveryType getPodRecoveryType(Collection<PodLaunch> podInstanceRequirements) {
        if (podInstanceRequirements.stream()
                .anyMatch(req -> req.getRecoveryType().equals(RecoveryType.PERMANENT))) {
            return RecoveryType.PERMANENT;
        } else {
            return RecoveryType.TRANSIENT;
        }
    }

    private RecoveryType getTaskRecoveryType(Collection<Protos.TaskInfo> taskInfos) {
        if (taskInfos.stream().allMatch(taskInfo -> isTaskPermanentlyFailed(taskInfo))) {
            return RecoveryType.PERMANENT;
        } else if (taskInfos.stream().noneMatch(taskInfo -> isTaskPermanentlyFailed(taskInfo))) {
            return RecoveryType.TRANSIENT;
        } else {
            for (Protos.TaskInfo taskInfo : taskInfos) {
                RecoveryType recoveryType =
                        isTaskPermanentlyFailed(taskInfo) ? RecoveryType.PERMANENT : RecoveryType.TRANSIENT;
                logger.info("Task: {} has recovery type: {}", taskInfo.getName(), recoveryType);
            }
            return RecoveryType.NONE;
        }
    }

    Step createStep(PodLaunch podLaunch) {
        logger.info("Creating step: {}", podLaunch);
        Optional<PodSpec> podSpec = serviceSpec.getPods().stream()
                .filter(spec -> spec.getType().equals(podLaunch.getId().getType()))
                .findAny();
        if (!podSpec.isPresent()) {
            throw new IllegalStateException(String.format("Unable to find pod spec for pod: %s", podLaunch.getId()));
        }
        return new RecoveryStep(
                podLaunch.getId().getName(),
                new PodInstance(podSpec.get(), podLaunch.getId().getIndex()),
                podLaunch,
                launchConstrainer,
                stateStore,
                namespace);
    }

    @Override
    public Set<PodLaunch> getDirtyAssets() {
        return PlanUtils.getDirtyAssets(plan);
    }

    private static List<String> getTaskNames(Collection<Protos.TaskInfo> taskInfos) {
        return taskInfos.stream()
                .map(taskInfo -> taskInfo.getName())
                .collect(Collectors.toList());
    }

    private static List<String> getPodNames(Collection<PodLaunch> podLaunches) {
        return podLaunches.stream()
                .map(podLaunch -> String.format("%s:%s", podLaunch.getId().getName(), podLaunch.getTasksToLaunch()))
                .collect(Collectors.toList());
    }
}
