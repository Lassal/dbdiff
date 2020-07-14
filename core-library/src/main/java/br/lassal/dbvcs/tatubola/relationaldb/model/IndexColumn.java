package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Comparator;
import java.util.Objects;

@JsonPropertyOrder({"name","order","ordinalPosition"})
public class IndexColumn extends Column {

    public static final Comparator DEFAULT_SORT_ORDER = Comparator.comparing(IndexColumn::getOrdinalPosition)
            .thenComparing(IndexColumn::getName);


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
