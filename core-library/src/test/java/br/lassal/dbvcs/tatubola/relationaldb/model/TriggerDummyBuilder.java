package br.lassal.dbvcs.tatubola.relationaldb.model;

public class TriggerDummyBuilder {


    public Trigger createSampleTrigger(int id, String schema, String targetObjectName){
        int type = id % 3;

        switch (type){
            case 1 : return this.createSampleTriggerA(id, schema, targetObjectName);
            case 2 : return this.createSampleTriggerB(id, schema, targetObjectName);
            default: return this.createSampleTriggerC(id, schema, targetObjectName);
        }
    }

    /**
     * Create sample trigger for the test.
     * The trigger body is not formatted, it will be formatted during the test
     * @param id
     * @param schema
     * @param targetObjectName
     * @return
     */
    private Trigger createSampleTriggerA(int id, String schema, String targetObjectName){
        Trigger trigger = new Trigger("TRIGGER_A_" + id);
        trigger.setTargetObjectType("TABLE");
        trigger.setTargetObjectSchema(schema);
        trigger.setTargetObjectName(targetObjectName);
        trigger.setExecutionOrder(id);
        trigger.setEvent("UPDATE");
        trigger.setEventTiming("AFTER EACH ROW");
        trigger.setEventActionBody("  BEGIN add_job_history(:old.employee_id,:old.hire_date,sysdate,:old.job_id," +
                ":old.department_id);END;");

        return trigger;
    }

    private Trigger createSampleTriggerB(int id, String schema, String targetObjectName){
        Trigger trigger = new Trigger("TRIGGER_B_" + id);
        trigger.setTargetObjectType("TABLE");
        trigger.setTargetObjectSchema(schema);
        trigger.setTargetObjectName(targetObjectName);
        trigger.setExecutionOrder(id);
        trigger.setEvent("INSERT OR UPDATE OR DELETE");
        trigger.setEventTiming("BEFORE STATEMENT");
        trigger.setEventActionBody("  BEGIN secure_dml; END secure_employees;");

        return trigger;
    }

    private Trigger createSampleTriggerC(int id, String schema, String targetObjectName){
        Trigger trigger = new Trigger("TRIGGER_C_" + id);
        trigger.setTargetObjectType("TABLE");
        trigger.setTargetObjectSchema(schema);
        trigger.setTargetObjectName(targetObjectName);
        trigger.setExecutionOrder(id);
        trigger.setEvent("INSERT on ROW");
        trigger.setEventTiming("BEFORE");
        trigger.setEventActionBody("BEGIN SET NEW.entryDate = NOW(); END");

        return trigger;
    }

}
