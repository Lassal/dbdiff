package br.lassal.dbvcs.tatubola.relationaldb.model;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UniqueConstraint extends TableConstraint {

    private List<Column> orderedColumns;

    public UniqueConstraint() {
    }

    public UniqueConstraint(String constraintSchema, String tableName, String constraintName, ConstraintType type) {
        super(constraintSchema, tableName, constraintName, type);
        this.orderedColumns = new ArrayList<>();

    }

    public void addColumn(Column column) {
        this.orderedColumns.add(column);

    }

    public List<Column> getColumns() {
        return this.orderedColumns;
    }

    public void setColumns(List<Column> columns) {
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
        String constraintColumns = String.join(", ", this.orderedColumns.stream().map(Column::getName).collect(Collectors.toList()));
        constStr.append("(" + constraintColumns + ")");
        return constStr.toString();
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof UniqueConstraint) {
            UniqueConstraint other = (UniqueConstraint) obj;

            isEqual = super.equals(other);
            isEqual &= (this.orderedColumns != null) && (other.orderedColumns != null)
                    && (this.orderedColumns.size() == other.orderedColumns.size());

            if (isEqual) {
                Map<String, Column> thisColumns = this.orderedColumns.stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));

                for (Column col : other.orderedColumns) {
                    isEqual &= thisColumns.containsKey(col.getName());

                    if (isEqual) {
                        Column thisCol = thisColumns.get(col.getName());
                        isEqual &= thisCol.getName().equals(col.getName());
                        isEqual &= thisCol.getOrdinalPosition() == col.getOrdinalPosition();
                    }
                }
            }
        }

        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderedColumns);
    }
}
