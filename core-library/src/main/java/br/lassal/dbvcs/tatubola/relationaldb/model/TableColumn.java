package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name","order","dataType", "textMaxLength", "numericPrecision", "numericScale", "nullable", "defaultValue"})
public class TableColumn extends TypedColumn {

    private boolean nullable;
    private String defaultValue;

    public TableColumn() {
    }

    public TableColumn(String name) {
        super(name);
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String toString() {
        String nullableText = this.nullable ? "NULL" : "NOT-NULL";
        String dataTypeSize = this.getDataTypeLength();

        return String.format("Name: %s|Datatype: %s(%s) |Nullable: %s|Position: %s | Default value: %s", this.getName(), this.getDataType()
                , dataTypeSize, nullableText, this.getOrdinalPosition(), this.defaultValue);
    }

    @Override
    public boolean equals(Object other) {
        boolean isEqual = false;

        if (other instanceof TableColumn) {
            TableColumn otherC = (TableColumn) other;
            isEqual = true;

            isEqual &= this.getName().equals(otherC.getName());
            isEqual &= this.getDataType().equals(otherC.getDataType());
            isEqual &= this.getOrdinalPosition() == otherC.getOrdinalPosition();
            isEqual &= (this.getTextMaxLength() == null && otherC.getTextMaxLength() == null) ||
                    (this.getTextMaxLength() != null && this.getTextMaxLength().equals(otherC.getTextMaxLength()));
            isEqual &= (this.getNumericPrecision() == null && otherC.getNumericPrecision() == null) ||
                    (this.getNumericPrecision() != null && this.getNumericPrecision().equals(otherC.getNumericPrecision()));
            isEqual &= (this.getNumericScale() == null && otherC.getNumericScale() == null) ||
                    (this.getNumericScale() != null && this.getNumericScale().equals(otherC.getNumericScale()));
            isEqual &= (this.getDefaultValue() == null && otherC.getDefaultValue() == null) ||
                    (this.getDefaultValue() != null && this.getDefaultValue().equals(otherC.getDefaultValue()));

        }

        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), nullable, defaultValue);
    }
}
