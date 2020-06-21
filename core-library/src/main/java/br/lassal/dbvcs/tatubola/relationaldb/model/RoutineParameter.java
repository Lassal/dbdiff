package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true, value={"routineSchema", "routineName", "routineID", "dataTypeLength"})
public class RoutineParameter extends TypedColumn{

    private String routineSchema;
    private String routineName;
    private ParameterMode parameterMode;

    public RoutineParameter(){
    }

    public RoutineParameter(String name, int ordinalPosition, String dataType, ParameterMode parameterMode) {
        super(name, ordinalPosition, dataType);
        this.setParameterMode(parameterMode);
    }



    public ParameterMode getParameterMode() {
        return parameterMode;
    }

    public void setParameterMode(ParameterMode parameterMode) {
        this.parameterMode = parameterMode;
    }

    public String getRoutineSchema() {
        return routineSchema;
    }

    public void setRoutineSchema(String routineSchema) {
        this.routineSchema = routineSchema;
    }

    public String getRoutineName() {
        return routineName;
    }

    public void setRoutineName(String routineName) {
        this.routineName = routineName;
    }

    public String getRoutineID(){
        return (this.routineSchema + "." + this.routineName).toUpperCase();
    }

    @Override
    public String toString(){

        return String.format("Name: %s|Datatype: %s(%s) |Param-mode: %s|Position: %s", this.getName(), this.getDataType()
                , this.getDataTypeLength(),  this.getParameterMode(), this.getOrdinalPosition());

    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if(obj instanceof RoutineParameter){
            RoutineParameter other = (RoutineParameter) obj;
            isEqual = super.equals(other);

            isEqual &= this.parameterMode.equals(other.parameterMode);
        }

        return isEqual;
    }
}
