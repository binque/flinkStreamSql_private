package com.cj.flink.sql.parser;

import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.calcite.FlinkTypeFactory;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.tools.FrameworkConfig;

//TODO  这里需要研究
public class FlinkPlanner {

    public static volatile FlinkPlannerImpl flinkPlanner;

    private FlinkPlanner() {
    }

    public static FlinkPlannerImpl createFlinkPlanner(FrameworkConfig frameworkConfig, RelOptPlanner relOptPlanner, FlinkTypeFactory typeFactory) {
        if (flinkPlanner == null) {
            synchronized (FlinkPlanner.class) {
                if (flinkPlanner == null) {
                    flinkPlanner = new FlinkPlannerImpl(frameworkConfig, relOptPlanner, typeFactory);
                }
            }
        }
        return flinkPlanner;
    }

    public static FlinkPlannerImpl getFlinkPlanner() {
        return flinkPlanner;
    }
}
