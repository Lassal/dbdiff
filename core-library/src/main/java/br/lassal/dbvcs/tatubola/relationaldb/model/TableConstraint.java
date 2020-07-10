package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "classtype")
@JsonSubTypes({
        @Type(value = CheckConstraint.class, name = "check-constraint"),
        @Type(value = UniqueConstraint.class, name = "unique-constraint"),
        @Type(value = ForeignKeyConstraint.class, name = "fk-constraint")
})
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true, value={"tableID","schema"})
public abstract class TableConstraint {

    private String schema;
    private String name;
    private ConstraintType type;
    private String tableName;

    public TableConstraint(){

    }

    public TableConstraint(String schema, String tableName, String constraintName, ConstraintType type){
        this.schema = schema;
        this.name = constraintName;
        this.type = type;
        this.tableName = tableName;
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableID() {
        return this.schema + "." + this.tableName;
    }

    public ConstraintType getType() {
        return type;
    }

    public void setType(ConstraintType type) {
        this.type = type;
    }

    public abstract void onAfterLoad();

    @Override
    public String toString(){
        return String.format("Type: %s|Name: %s|", this.type, this.name);
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if(obj instanceof TableConstraint){
            TableConstraint other = (TableConstraint) obj;
            isEqual = true;

            isEqual &= this.schema.equals(other.schema);
            isEqual &= this.tableName.equals(other.tableName);
            isEqual &= this.name.equals(other.name);
            isEqual &= this.type.equals(other.type);

        }

        return isEqual;
    }

    @Override
    public int hashCode(){
        return Objects.hash(this.schema, this.tableName, this.name, this.type);
    }
}
