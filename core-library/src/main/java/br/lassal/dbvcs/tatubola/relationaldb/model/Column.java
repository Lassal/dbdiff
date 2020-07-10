package br.lassal.dbvcs.tatubola.relationaldb.model;

import java.util.Objects;

public class Column {

    private String name;
    private int ordinalPosition;

    public Column(){

    }

    public Column(String name, int ordinalPosition){
        this.name = name;
        this.ordinalPosition = ordinalPosition;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(int ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if(obj instanceof Column){
            Column other = (Column) obj;
            isEqual = true;

            isEqual &= this.name.equals(other.name);
            isEqual &= this.ordinalPosition == other.ordinalPosition;
        }

        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ordinalPosition);
    }
}
