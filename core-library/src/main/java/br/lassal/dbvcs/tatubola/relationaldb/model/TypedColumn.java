package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TypedColumn extends Column {
    private String dataType;
    private Long textMaxLength;
    private Integer numericPrecision;
    private Integer numericScale;

    public TypedColumn() {
    }

    public TypedColumn(String name, int ordinalPosition, String dataType) {
        super(name, ordinalPosition);
        this.setDataType(dataType);
    }

    public TypedColumn(String name) {
        this.setName(name);
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Long getTextMaxLength() {
        return textMaxLength;
    }

    public void setTextMaxLength(Long textMaxLength) {
        this.textMaxLength = textMaxLength;
    }

    public Integer getNumericPrecision() {
        return numericPrecision;
    }

    public void setNumericPrecision(Integer numericPrecision) {
        this.numericPrecision = numericPrecision;
    }

    public Integer getNumericScale() {
        return numericScale;
    }

    public void setNumericScale(Integer numericScale) {
        this.numericScale = numericScale;
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof TypedColumn) {
            TypedColumn other = (TypedColumn) obj;
            isEqual = super.equals(other);

            isEqual &= this.dataType.equals(other.dataType);
            isEqual &= (this.textMaxLength == null && other.textMaxLength == null) ||
                    (this.textMaxLength != null && this.textMaxLength.equals(other.textMaxLength));
            isEqual &= (this.numericPrecision == null && other.numericPrecision == null) ||
                    (this.numericPrecision != null && this.numericPrecision.equals(other.numericPrecision));
            isEqual &= (this.numericScale == null && other.numericScale == null) ||
                    (this.numericScale != null && this.numericScale.equals(other.numericScale));
        }

        return isEqual;
    }

    public String getDataTypeLength() {
        if (this.getTextMaxLength() != null) {
            return this.getTextMaxLength().toString();
        }

        if (this.getNumericPrecision() != null) {
            if (this.getNumericScale() != null) {
                return this.getNumericPrecision() + "," + this.getNumericScale();
            } else {
                return this.getNumericPrecision().toString();
            }
        }

        return "";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dataType, textMaxLength, numericPrecision, numericScale);
    }
}
