package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@JsonPropertyOrder({"name", "type", "columns"})
public class ForeignKeyConstraint extends TableConstraint implements Cloneable {

    private List<ReferentialIntegrityColumn> orderedColumns;

    public ForeignKeyConstraint() {

    }

    public ForeignKeyConstraint(String constraintSchema, String tableName, String constraintName) {
        super(constraintSchema, tableName, constraintName, ConstraintType.FOREIGN_KEY);
        this.orderedColumns = new ArrayList<>();
    }

    public void addColumn(ReferentialIntegrityColumn column) {
        this.orderedColumns.add(column);

    }

    public List<ReferentialIntegrityColumn> getColumns() {
        return this.orderedColumns;
    }

    public void setColumns(List<ReferentialIntegrityColumn> columns) {
        this.orderedColumns = columns;
    }

    public void sortColumns() {
        this.orderedColumns.sort(Comparator.comparingInt(Column::getOrdinalPosition));
    }


    @Override
    public void onAfterLoad() {
        this.sortColumns();
    }

    @Override
    public String toString() {
        StringBuilder constStr = new StringBuilder(super.toString());
        String constraintColumns = String.join(", ", this.orderedColumns.stream().map(ReferentialIntegrityColumn::toString).collect(Collectors.toList()));
        constStr.append("(" + constraintColumns + ")");
        return constStr.toString();

    }

    @Override
    public boolean equals(Object obj) {
        return this.contentAndOrderEquals(obj);
    }

    public boolean contentEquals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof ForeignKeyConstraint) {
            ForeignKeyConstraint other = (ForeignKeyConstraint) obj;

            isEqual = super.equals(other);
            isEqual &= (this.orderedColumns != null) && (other.orderedColumns != null)
                    && (this.orderedColumns.size() == other.orderedColumns.size());

            if (isEqual) {
                Map<String, ReferentialIntegrityColumn> thisColumns = this.orderedColumns.stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));

                for (ReferentialIntegrityColumn col : other.orderedColumns) {
                    isEqual &= thisColumns.containsKey(col.getName());

                    if (isEqual) {
                        ReferentialIntegrityColumn thisCol = thisColumns.get(col.getName());
                        isEqual &= thisCol.getName().equals(col.getName());
                        isEqual &= thisCol.getOrdinalPosition() == col.getOrdinalPosition();
                        isEqual &= thisCol.getReferencedSchemaName().equals(col.getReferencedSchemaName());
                        isEqual &= thisCol.getReferencedTableName().equals(col.getReferencedTableName());
                        isEqual &= thisCol.getReferencedTableColumnName().equals(col.getReferencedTableColumnName());
                    }
                }
            }
        }

        return isEqual;
    }

    public boolean contentAndOrderEquals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof ForeignKeyConstraint) {
            ForeignKeyConstraint other = (ForeignKeyConstraint) obj;

            isEqual = super.equals(other);
            isEqual &= (this.orderedColumns != null) && (other.orderedColumns != null)
                    && (this.orderedColumns.size() == other.orderedColumns.size());

            if (isEqual) {

                for (int i = 0; i < this.orderedColumns.size(); i++) {
                    ReferentialIntegrityColumn thisCol = this.orderedColumns.get(i);
                    ReferentialIntegrityColumn otherCol = this.orderedColumns.get(i);

                    isEqual &= thisCol.getName().equals(otherCol.getName());
                    isEqual &= thisCol.getOrdinalPosition() == otherCol.getOrdinalPosition();
                    isEqual &= thisCol.getReferencedSchemaName().equals(otherCol.getReferencedSchemaName());
                    isEqual &= thisCol.getReferencedTableName().equals(otherCol.getReferencedTableName());
                    isEqual &= thisCol.getReferencedTableColumnName().equals(otherCol.getReferencedTableColumnName());
                }
            }
        }

        return isEqual;
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderedColumns);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
