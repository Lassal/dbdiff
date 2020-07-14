package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder({"name", "ordinalPosition", "referencedSchemaName", "referencedTableName", "referencedTableColumnName" })
public class ReferentialIntegrityColumn extends Column {

    private String referencedSchemaName;
    private String referencedTableName;
    private String referencedTableColumnName;


    public ReferentialIntegrityColumn() {
    }

    public ReferentialIntegrityColumn(String constraintColumnName, int ordinalPosition) {
        super(constraintColumnName, ordinalPosition);
    }

    public String getReferencedSchemaName() {
        return referencedSchemaName;
    }

    public void setReferencedSchemaName(String referencedSchemaName) {
        this.referencedSchemaName = referencedSchemaName;
    }

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public void setReferencedTableName(String referencedTableName) {
        this.referencedTableName = referencedTableName;
    }

    public String getReferencedTableColumnName() {
        return referencedTableColumnName;
    }

    public void setReferencedTableColumnName(String referencedTableColumnName) {
        this.referencedTableColumnName = referencedTableColumnName;
    }

    public String toString() {
        return String.format("%s -> %s.%s.%s", this.getName(), this.referencedSchemaName, this.referencedTableName, this.referencedTableColumnName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReferentialIntegrityColumn)) return false;
        if (!super.equals(o)) return false;
        ReferentialIntegrityColumn that = (ReferentialIntegrityColumn) o;
        return Objects.equals(getReferencedSchemaName(), that.getReferencedSchemaName()) &&
                Objects.equals(getReferencedTableName(), that.getReferencedTableName()) &&
                Objects.equals(getReferencedTableColumnName(), that.getReferencedTableColumnName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getReferencedSchemaName(), getReferencedTableName(), getReferencedTableColumnName());
    }
}
