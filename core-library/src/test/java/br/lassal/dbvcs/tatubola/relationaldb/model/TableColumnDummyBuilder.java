package br.lassal.dbvcs.tatubola.relationaldb.model;

public class TableColumnDummyBuilder {

    public TableColumn createTextColumn(int orderId, long maxLength, boolean isNullable){
        TableColumn column = this.createTableColumn(orderId, "VARCHAR", isNullable);
        column.setTextMaxLength(maxLength);
        column.setDefaultValue("---<DEFAULT>---");

        return column;
    }

    public TableColumn createNumericColumn(int orderId, int numericPrecison, int numericScale, boolean isNullable){
        TableColumn column = this.createTableColumn(orderId, "NUMBER", isNullable);
        column.setNumericPrecision(numericPrecison);
        column.setNumericScale(numericScale);

        return column;
    }

    private TableColumn createTableColumn(int orderId, String datatype, boolean isNullable){
        TableColumn column = new TableColumn(String.format("COLUMN%02d", orderId));
        column.setOrdinalPosition(orderId);
        column.setDataType(datatype);
        column.setNullable(isNullable);

        return column;
    }

}
