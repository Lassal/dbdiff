package br.lassal.dbvcs.tatubola.relationaldb.model.sort;

import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;

import java.util.Comparator;

public class TriggerComparator implements Comparator<Trigger> {

    static {
        comparator = new TriggerComparator();
    }

    private static final TriggerComparator comparator;

    public static final TriggerComparator getSingleton() {
        return comparator;
    }

    @Override
    public int compare(Trigger firstTrigger, Trigger secondTrigger) {
        String firstTriggerOrder = this.getPartitionOrder(firstTrigger);
        String secondTriggerOrder = this.getPartitionOrder(secondTrigger);
        return firstTriggerOrder.compareTo(secondTriggerOrder);
    }

    private String getPartitionOrder(Trigger trigger) {
        return String.format("[%s].[%s].[%s].[%s].%09d"
                , trigger.getTargetObjectSchema()
                , trigger.getTargetObjectName()
                , trigger.getEvent()
                , trigger.getEventTiming()
                , trigger.getExecutionOrder());
    }
}
