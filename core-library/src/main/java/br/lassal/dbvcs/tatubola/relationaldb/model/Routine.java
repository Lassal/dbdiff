package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.text.SqlNormalizer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@JsonPropertyOrder({"schema","name", "routineType", "returnParamater", "parameters", "routineDefinition"})
@JsonIgnoreProperties(ignoreUnknown = true, value = {"routineID"})
public class Routine implements DatabaseModelEntity, Cloneable {

    public static String getRoutineID(String schema, String name) {
        return (schema + "." + name).toUpperCase();
    }

    private String schema;
    private String name;
    private RoutineType routineType;
    private TypedColumn returnParamater;
    private List<RoutineParameter> parameters;
    private String routineDefinition;

    public Routine() {

    }

    public Routine(String schema, String name, RoutineType routineType) {
        this.schema = schema;
        this.name = name;
        this.routineType = routineType;
        this.parameters = new ArrayList<>();
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    @Override
    public void tidyUpProperties(SqlNormalizer normalizer) {

        if(this.routineType.equals(RoutineType.PACKAGE)){
            this.parameters.sort(Comparator.comparing(RoutineParameter::getPackageRoutineName)
            .thenComparing(RoutineParameter::getOrdinalPosition));
        }
        else {
            this.parameters.sort(RoutineParameter.DEFAULT_SORT_ORDER);
        }

        this.routineDefinition = normalizer.formatSql(this.routineDefinition);
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRoutineID() {
        return Routine.getRoutineID(this.schema, this.name);
    }

    public RoutineType getRoutineType() {
        return routineType;
    }

    public void setRoutineType(RoutineType routineType) {
        this.routineType = routineType;
    }

    public TypedColumn getReturnParamater() {
        return returnParamater;
    }

    public void setReturnParamater(TypedColumn returnParamater) {
        this.returnParamater = returnParamater;
    }

    public List<RoutineParameter> getParameters() {
        return this.parameters;
    }


    public String getRoutineDefinition() {
        return routineDefinition;
    }

    public void setRoutineDefinition(String routineDefinition) {
        this.routineDefinition = routineDefinition;
    }

    public void addParameter(RoutineParameter param) {
        this.parameters.add(param);
    }

    @Override
    public String toString() {
        StringBuilder routine = new StringBuilder();

        routine.append(String.format("Schema: %s | Routine: %s%n", this.schema, this.name));
        routine.append(String.format("Type: %s ", this.routineType));

        if (RoutineType.FUNCTION.equals(this.routineType)) {
            routine.append(String.format("| RETURN TYPE -> %s(%s)"
                    , this.returnParamater.getDataType(), this.returnParamater.getDataTypeLength()));
        }
        routine.append("\n");

        for (RoutineParameter param : this.parameters) {
            routine.append(String.format("    %s%n", param));
        }

        routine.append("\n----- BODY -----\n");
        routine.append(this.routineDefinition);

        return routine.toString();
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof Routine) {
            Routine other = (Routine) obj;
            isEqual = true;

            isEqual &= this.schema.equals(other.schema);
            isEqual &= this.name.equals(other.name);
            isEqual &= this.routineType.equals(other.routineType);
            isEqual &= (this.returnParamater == null)
                    || (this.returnParamater.getDataType().equals(other.returnParamater.getDataType()));
            isEqual &= this.routineDefinition.equals(other.routineDefinition);
            isEqual &= this.parameters.size() == other.parameters.size();

            if (isEqual) {
                Map<String, RoutineParameter> thisParams = this.parameters.stream()
                        .collect(Collectors.toMap(RoutineParameter::getName, Function.identity()));

                for (RoutineParameter param : other.getParameters()) {
                    isEqual &= thisParams.containsKey(param.getName());

                    if (isEqual) {
                        isEqual &= thisParams.get(param.getName()).equals(param);
                    }
                }

            }

        }

        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, routineType, returnParamater, parameters, routineDefinition);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
