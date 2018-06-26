package com.mesosphere.sdk.hdfs.scheduler;

import com.mesosphere.sdk.scheduler.plan.*;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.scheduler.recovery.RecoveryStep;
import com.mesosphere.sdk.scheduler.recovery.RecoveryPlanOverrider;
import com.mesosphere.sdk.scheduler.recovery.RecoveryType;
import com.mesosphere.sdk.scheduler.recovery.constrain.UnconstrainedLaunchConstrainer;
import com.mesosphere.sdk.specification.PodId;
import com.mesosphere.sdk.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * The HdfsRecoveryPlanManager handles failure scenarios unique to HDFS.  It falls back to the default recovery behavior
 * when appropriate.
 */
public class HdfsRecoveryPlanOverrider implements RecoveryPlanOverrider {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String PHASE_NAME_TEMPLATE = "permanent-%s-failure-recovery";
    private static final String NN_PHASE_NAME = "name";
    private static final String JN_PHASE_NAME = "journal";
    private final StateStore stateStore;
    private final Plan replacePlan;

    public HdfsRecoveryPlanOverrider(StateStore stateStore, Plan replacePlan) {
        this.stateStore = stateStore;
        this.replacePlan = replacePlan;
    }

    @Override
    public Optional<Phase> override(PodLaunch stoppedPod) {
        PodId podId = stoppedPod.getId();

        if (podId.getType().equals("data") || stoppedPod.getRecoveryType() != RecoveryType.PERMANENT) {
            logger.info("No overrides necessary. Pod is not a journal or name node, or failure isn't permanent");
            return Optional.empty();
        }

        if (podId.getType().equals("name")) {
            if (podId.getIndex() <= 1) {
                logger.info("Returning replacement plan for name node {}", podId.getIndex());
                return Optional.of(getRecoveryPhase(replacePlan, podId.getIndex(), NN_PHASE_NAME));
            } else {
                logger.error("Encountered unexpected index: {}, falling back to default recovery plan manager",
                        podId.getName());
                return Optional.empty();
            }
        } else {
            if (podId.getIndex() <= 2) {
                logger.info("Returning replacement plan for journal node {}", podId.getIndex());
                return Optional.of(getRecoveryPhase(replacePlan, podId.getIndex(), JN_PHASE_NAME));
            } else {
                logger.error("Encountered unexpected index: {}, falling back to default recovery plan manager",
                        podId.getName());
                return Optional.empty();
            }
        }
    }

    private Phase getRecoveryPhase(Plan inputPlan, int index, String phaseName) {
        Phase inputPhase = getPhaseForNodeType(inputPlan, phaseName);
        int offset = index * 2;

        // Bootstrap
        Step inputBootstrapStep = inputPhase.getChildren().get(offset + 0);
        Step bootstrapStep = new RecoveryStep(
                inputBootstrapStep.getName(),
                inputBootstrapStep.getPodInstance().get(),
                PodLaunch.newBuilder(inputBootstrapStep.getPodLaunch().get())
                        .recoveryType(RecoveryType.PERMANENT)
                        .build(),
                new UnconstrainedLaunchConstrainer(),
                stateStore);

        // JournalNode or NameNode
        Step inputNodeStep = inputPhase.getChildren().get(offset + 1);
        Step nodeStep = new RecoveryStep(
                inputNodeStep.getName(),
                inputNodeStep.getPodInstance().get(),
                PodLaunch.newBuilder(inputNodeStep.getPodLaunch().get())
                        .recoveryType(RecoveryType.TRANSIENT)
                        .build(),
                new UnconstrainedLaunchConstrainer(),
                stateStore);

        return new DefaultPhase(
                String.format(PHASE_NAME_TEMPLATE, phaseName),
                Arrays.asList(bootstrapStep, nodeStep),
                new SerialStrategy<>(),
                Collections.emptyList());
    }

    private static Phase getPhaseForNodeType(Plan inputPlan, String phaseName) {
        Optional<Phase> phaseOptional = inputPlan.getChildren().stream()
                .filter(phase -> phase.getName().equals(phaseName))
                .findFirst();

        if (!phaseOptional.isPresent()) {
            throw new IllegalStateException(
                    String.format("Expected phase name %s does not exist in the service spec plan", phaseName));
        }

        return phaseOptional.get();
    }
}
