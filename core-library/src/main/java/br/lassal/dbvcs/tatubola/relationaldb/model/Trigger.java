package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.text.SqlNormalizer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder({"schema", "name", "targetObjectSchema", "targetObjectName", "targetObjectType", "eventTiming"
        , "event", "executionOrder", "eventActionBody"})
@JsonIgnoreProperties(ignoreUnknown = true, value = {"schema", "triggerID"})
public class Trigger implements DatabaseModelEntity {

    private String targetObjectSchema;
    private String targetObjectName;
    private String targetObjectType;
    private String name;
    private String eventTiming;
    private String event;
    private String eventActionBody;

    private int executionOrder;


    public Trigger() {
    }

    public Trigger(String name) {
        this.name = name;
    }

    /**
     * Returns the schema of the target object that
     * this trigger is linked/attached
     *
     * @return
     */
    public String getTargetObjectSchema() {
        return targetObjectSchema;
    }

    public void setTargetObjectSchema(String targetObjectSchema) {
        this.targetObjectSchema = targetObjectSchema;
    }

    /**
     * Returns the name of the target object that
     * this trigger is linked/attached
     *
     * @return
     */
    public String getTargetObjectName() {
        return targetObjectName;
    }

    public void setTargetObjectName(String targetObjectName) {
        this.targetObjectName = targetObjectName;
    }

    /**
     * Return the object type of the object that this
     * trigger is linked/attached.
     * Usually the type is TABLE, but Oracle allows to
     * create triggers for VIEWS, SCHEMA and also DATABASE
     * IMPORTANT: we will not map DATABASE triggers in this first
     * release because it doesn't relate to problems of versioning for
     * applications in general
     *
     * @return
     */
    public String getTargetObjectType() {
        return targetObjectType;
    }

    public void setTargetObjectType(String targetObjectType) {
        this.targetObjectType = targetObjectType;
    }

    @Override
    public String getSchema() {
        return this.targetObjectSchema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void tidyUpProperties(SqlNormalizer normalizer) {
        //throw new UnsupportedOperationException();
    }


    public String getEventTiming() {
        return eventTiming;
    }

    public void setEventTiming(String eventTiming) {
        this.eventTiming = eventTiming;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getEventActionBody() {
        return eventActionBody;
    }

    public void setEventActionBody(String eventActionBody) {
        this.eventActionBody = eventActionBody;
    }

    public void setExecutionOrder(int executionOrder) {
        this.executionOrder = executionOrder;
    }

    public int getExecutionOrder() {
        return executionOrder;
    }

    public String getTriggerID() {
        return this.getSchema() + "." + this.name;
    }

    @Override
    public String toString() {
        StringBuilder trigger = new StringBuilder();
        trigger.append("TRIGGER: " + this.name + "\n");
        trigger.append(String.format("Target Object Type: %s| Target Object: %s.%s %n"
                , this.targetObjectType, this.targetObjectSchema, this.targetObjectName));
        trigger.append(String.format("Event: %s | Timing: %s | Execution order: %s %n"
                , this.event, this.eventTiming, this.executionOrder));
        trigger.append("ACTION: \n\n");
        trigger.append(this.eventActionBody);
        trigger.append("\n------------------X");

        return trigger.toString();
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof Trigger) {
            Trigger other = (Trigger) obj;
            isEqual = true;

            isEqual &= this.targetObjectSchema.equals(other.targetObjectSchema);
            isEqual &= this.targetObjectName.equals(other.targetObjectName);
            isEqual &= this.targetObjectType.equals(other.targetObjectType);
            isEqual &= this.name.equals(other.name);
            isEqual &= this.event.equals(other.event);
            isEqual &= this.eventTiming.equals(other.eventTiming);
            isEqual &= this.executionOrder == other.executionOrder;
            isEqual &= this.getEventActionBody().equals(other.getEventActionBody());
        }

        return isEqual;
    }


    @Override
    public int hashCode() {
        return Objects.hash(targetObjectSchema, targetObjectName, targetObjectType, name, eventTiming, event, eventActionBody, executionOrder);
    }
}


