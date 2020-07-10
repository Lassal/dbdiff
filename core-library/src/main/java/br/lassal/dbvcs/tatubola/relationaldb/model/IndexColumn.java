package br.lassal.dbvcs.tatubola.relationaldb.model;

import java.util.Objects;

public class IndexColumn extends Column {

    private ColumnOrder order;

    public IndexColumn() {

    }

    public IndexColumn(String name, int ordinalPosition, ColumnOrder sortOrder) {
        super(name, ordinalPosition);
        this.setOrder(sortOrder);
    }


    public ColumnOrder getOrder() {
        return order;
    }

    public void setOrder(ColumnOrder order) {
        this.order = order;
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof IndexColumn) {
            IndexColumn other = (IndexColumn) obj;
            isEqual = super.equals(other);

            isEqual &= this.order.equals(other.order);
        }

        return isEqual;

    }

    @Override
    public String toString() {
        return String.format("Name: %s|Position: %s|Order: %s", this.getName(), this.getOrdinalPosition(), this.getOrder());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), order);
    }
}
