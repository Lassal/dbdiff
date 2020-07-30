package br.lassal.dbvcs.tatubola.relationaldb.model;

public class TableDummyBuilder {

    /**
     * Create a simple test table with 7 different columns, and 4 constraints : PK, FK, Unique, Check
     *
     * @param schema Schema of the table belongs
     * @param tableName Table name
     * @param sequentialId an ID to be used an identifier during the table and property generation; can called many times
     *                     it would be recommended to have a different id for each table
     * @return
     */
    public Table createFullTable(String schema, String tableName, int sequentialId){
        TableColumnDummyBuilder colBuilder = new TableColumnDummyBuilder();
        Table table = new Table(schema, tableName);

        table.addColumn(colBuilder.createNumericColumn(1, 25, 5, false));
        table.addColumn(colBuilder.createTextColumn(2, 30, false));
        table.addColumn(colBuilder.createTextColumn(3, 40, true));
        table.addColumn(colBuilder.createNumericColumn(4, 18, 2, false));
        table.addColumn(colBuilder.createTextColumn(5, 50, true));
        table.addColumn(colBuilder.createNumericColumn(6, 10, 3, false));
        table.addColumn(colBuilder.createNumericColumn(7, 28, 7, true));

        table.addConstraint(this.createUniqueConstraint(schema, tableName, true, sequentialId));
        table.addConstraint(this.createFKConstraint(schema, tableName, sequentialId));
        table.addConstraint(this.createUniqueConstraint(schema, tableName, false, 100+ sequentialId));
        table.addConstraint(new CheckConstraint(schema, tableName, "DummyChkConstraint" + sequentialId, "'FIELD01 > 78'"));

        return table;
    }


    private UniqueConstraint createUniqueConstraint(String schema, String table, boolean isPK, int sequentialId){
        ConstraintType cType = isPK ? ConstraintType.PRIMARY_KEY : ConstraintType.UNIQUE;
        String constraintName = isPK ? "PK00" + sequentialId : "UNIQUE_" + sequentialId;
        UniqueConstraint constraint = new UniqueConstraint(schema, table, constraintName, cType);
        constraint.addColumn(new Column("COLUMN01", 1));
        constraint.addColumn(new Column("COLUMN02", 2));
        constraint.addColumn(new Column("COLUMN03", 3));

        return constraint;
    }

    private ForeignKeyConstraint createFKConstraint(String schema, String table, int sequentialId){
        ForeignKeyConstraint constraint = new ForeignKeyConstraint(schema, table, "FK_" + table + "00" + sequentialId);
        constraint.addColumn(this.createRefIntegrityColumn(1, "BBB", "OTHER_TABLE"));
        constraint.addColumn(this.createRefIntegrityColumn(2, "BBB", "OTHER_TABLE"));

        return constraint;
    }

    private ReferentialIntegrityColumn createRefIntegrityColumn(int sequentialID, String schema, String refTableName){
        ReferentialIntegrityColumn column = new ReferentialIntegrityColumn("COLUMN" + sequentialID, sequentialID);
        column.setReferencedSchemaName(schema);
        column.setReferencedTableName(refTableName);
        column.setReferencedTableColumnName(column.getName());

        return column;
    }
}
