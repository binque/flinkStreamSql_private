package com.cj.flink.sql.launcher.perjob;

import org.apache.flink.client.deployment.ClusterSpecification;

import com.cj.flink.sql.util.MathUtil;

import java.util.Properties;

public class FLinkPerJobResourceUtil {
    public final static int MIN_JM_MEMORY = 768; // the minimum memory should be higher than the min heap cutoff
    public final static int MIN_TM_MEMORY = 768;

    public final static String JOBMANAGER_MEMORY_MB = "jobmanager.memory.mb";
    public final static String TASKMANAGER_MEMORY_MB = "taskmanager.memory.mb";
    public final static String NUMBER_TASK_MANAGERS = "taskmanager.num";
    public final static String SLOTS_PER_TASKMANAGER = "taskmanager.slots";

    public static ClusterSpecification createClusterSpecification(Properties confProperties) {
        int jobmanagerMemoryMb = 768;
        int taskmanagerMemoryMb = 768;
        int numberTaskManagers = 1;
        int slotsPerTaskManager = 1;

        if (confProperties != null) {
            if (confProperties.containsKey(JOBMANAGER_MEMORY_MB)){
                jobmanagerMemoryMb = MathUtil.getIntegerVal(confProperties.get(JOBMANAGER_MEMORY_MB));
                if (jobmanagerMemoryMb < MIN_JM_MEMORY) {
                    jobmanagerMemoryMb = MIN_JM_MEMORY;
                }
            }

            if (confProperties.containsKey(TASKMANAGER_MEMORY_MB)){
                taskmanagerMemoryMb = MathUtil.getIntegerVal(confProperties.get(TASKMANAGER_MEMORY_MB));
                if (taskmanagerMemoryMb < MIN_TM_MEMORY) {
                    taskmanagerMemoryMb = MIN_TM_MEMORY;
                }
            }

            if (confProperties.containsKey(NUMBER_TASK_MANAGERS)){
                numberTaskManagers = MathUtil.getIntegerVal(confProperties.get(NUMBER_TASK_MANAGERS));
            }

            if (confProperties.containsKey(SLOTS_PER_TASKMANAGER)){
                slotsPerTaskManager = MathUtil.getIntegerVal(confProperties.get(SLOTS_PER_TASKMANAGER));
            }
        }

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobmanagerMemoryMb)
                .setTaskManagerMemoryMB(taskmanagerMemoryMb)
                .setNumberTaskManagers(numberTaskManagers)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }
}
