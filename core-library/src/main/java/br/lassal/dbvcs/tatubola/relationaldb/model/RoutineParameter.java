package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Comparator;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name","order","dataType", "textMaxLength", "numericPrecision", "numericScale","parameterMode"})
@JsonIgnoreProperties(ignoreUnknown = true, value = {"routineSchema", "routineName", "routineID", "dataTypeLength"
        , "packageRoutineName"})
public class RoutineParameter extends TypedColumn {

    public static final Comparator DEFAULT_SORT_ORDER = Comparator.comparing(RoutineParameter::getOrdinalPosition);

    private String routineSchema;
    private String routineName;
    private ParameterMode parameterMode;

    public RoutineParameter() {
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

    public String getRoutineID() {
        return (this.routineSchema + "." + this.routineName).toUpperCase();
    }

    /**
     * Helper method to return the routine name of package routines.
     * Package routines show many routines in the same file, to differentiate between the routines
     * the parameters receive the following format {routine name}{(number of overloads)}.{parameter name}
     * The first part containing the routine name and de overload allow  the parameters to be properly ordered
     * in the serialized output
     * @return The package routine name plus overload of the routine without the parameter name
     */
    public String getPackageRoutineName(){
        int routineMarker = this.getName().lastIndexOf('.');

        if(routineMarker > -1){
            return  this.getName().substring(0, routineMarker);
        }
        else{
            return null;
        }
    }

    @Override
    public String toString() {

        return String.format("Name: %s|Datatype: %s(%s) |Param-mode: %s|Position: %s", this.getName(), this.getDataType()
                , this.getDataTypeLength(), this.getParameterMode(), this.getOrdinalPosition());

    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof RoutineParameter) {
            RoutineParameter other = (RoutineParameter) obj;
            isEqual = super.equals(other);

            isEqual &= this.parameterMode.equals(other.parameterMode);
        }

        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), routineSchema, routineName, parameterMode);
    }
}
