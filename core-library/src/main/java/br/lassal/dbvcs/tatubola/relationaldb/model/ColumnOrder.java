package br.lassal.dbvcs.tatubola.relationaldb.model;

public enum ColumnOrder {
    ASC,
    DESC;


    public static ColumnOrder fromMySQL(String columnOrder) {
        if (columnOrder == null) {
            return null;
        }
        switch (columnOrder) {
            case "A":
                return ColumnOrder.ASC;
            case "D":
                return ColumnOrder.DESC;
            default:
                return null;
        }
    }

    public static ColumnOrder fromOracle(String columnOrder) {
        if (columnOrder == null) {
            return null;
        }
        switch (columnOrder) {
            case "ASC":
                return ColumnOrder.ASC;
            case "DESC":
                return ColumnOrder.DESC;
            default:
                return null;
        }
    }
}
