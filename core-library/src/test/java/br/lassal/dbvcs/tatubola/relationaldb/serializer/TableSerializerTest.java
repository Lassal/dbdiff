package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TableSerializerTest extends BaseSerializerTest{

    /**
     * Test if the TableSerializer is able to
     *  - assemble tables from separated parts: table columns + constraints
     *  - tidy up the fields and sort them in the proper order
     *  - serialize its content to DBModelFS
     *
     * @throws Exception
     */
    @Test
    public void testTableSerialization() throws Exception {
        String env = "DEV";
        String schema = "AAA";
        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();

        TableSerializer serializer = new TableSerializer(this.repository, dbModelFS, schema, env);

        List<Table> sourceTables = new ArrayList<>();
        sourceTables.add(this.createFullTable(schema, "TABLE001", 1));
        sourceTables.add(this.createFullTable(schema, "TABLE002", 2));
        sourceTables.add(this.createFullTable(schema, "TABLE003", 3));
        sourceTables.add(this.createFullTable(schema, "TABLE004", 4));
        sourceTables.add(this.createFullTable(schema, "TABLE005", 5));


        when(this.repository.loadTableColumns(schema)).thenReturn(this.extractTableColumnsOnly(sourceTables));
        when(this.repository.loadCheckConstraints(schema)).thenReturn(this.extractCheckConstraints(sourceTables));
        when(this.repository.loadUniqueConstraints(schema)).thenReturn(this.extractUniqueConstraints(sourceTables));
        when(this.repository.loadReferentialConstraints(schema)).thenReturn(this.extractFKConstraints(sourceTables));

        serializer.serialize();

        for(Table originalTable: sourceTables){
            InMemoryTestDBModelFS.SerializationInfo serializationInfo = dbModelFS.getSerializationInfo(originalTable);

            assertEquals(originalTable, serializationInfo.getDBObject());
        }

        verify(this.repository, times(1)).loadTableColumns(schema);
        verify(this.repository, times(1)).loadCheckConstraints(schema);
        verify(this.repository, times(1)).loadUniqueConstraints(schema);
        verify(this.repository, times(1)).loadReferentialConstraints(schema);

        assertEquals(sourceTables.size(), dbModelFS.getNumberSerializedObjects());
    }

    /**
     * Test if the output generated by the TableSerializer can be deserialized and it
     * has the same properties than the serialized version
     * @throws Exception
     */
    @Test
    public void testSerializeDeserializeTable() throws Exception {
        String env = "DEV";
        String schema = "XPTO";
        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();
        TableSerializer serializer = new TableSerializer(this.repository, dbModelFS, schema, env);

        List<Table> sourceTables = new ArrayList<>();
        sourceTables.add(this.createFullTable(schema, "TABLE001", 1));

        when(this.repository.loadTableColumns(schema)).thenReturn(this.extractTableColumnsOnly(sourceTables));
        when(this.repository.loadCheckConstraints(schema)).thenReturn(this.extractCheckConstraints(sourceTables));
        when(this.repository.loadUniqueConstraints(schema)).thenReturn(this.extractUniqueConstraints(sourceTables));
        when(this.repository.loadReferentialConstraints(schema)).thenReturn(this.extractFKConstraints(sourceTables));

        serializer.serialize();

        Table sourceTable = sourceTables.get(0);
        InMemoryTestDBModelFS.SerializationInfo serializedTableInfo = dbModelFS.getSerializationInfo(sourceTable);

        assertEquals(sourceTable, serializedTableInfo.getDBObject());

        Table deserializedTable = this.textSerializer.fromYAMLtoPOJO(serializedTableInfo.getYamlText(), Table.class);

        assertEquals(sourceTable, deserializedTable);

    }


    /**
     * Disassemble each table creating a copy of the table with only the columns.
     * Shuffles all columns to create a situation where they were load from the
     * database unordered
     * This method return the tables in the same format expected by
     * RelationalDBRepository.loadTableColumns
     *
     * @param sourceTables A list of fully filled tables
     * @return a Map with Key Schema.TableName, and value Table instance
     */
    private Map<String, Table> extractTableColumnsOnly(List<Table> sourceTables)  {
        Map<String, Table> tableColumns = new HashMap<>();

        for(Table table: sourceTables){
           Table copy = new Table(table.getSchema(), table.getName());
           List<TableColumn> columns = new ArrayList<>(table.getColumns());
           Collections.shuffle(columns);
           columns.stream().forEach(c -> copy.addColumn(c));

           tableColumns.put(copy.getTableID(), copy);
        }

        return tableColumns;
    }

    /**
     * Extract a list of Unique constraints from a list of expected tables
     * For each UniqueConstraint in the table list create a copy and shuffle the column order
     * to test how the assembler will order it
     *
     * It returns a list in the format defined at RelationalDBRepository.loadUniqueConstraints
     * @param sourceTables A list of fully filled tables
     * @return A list of unique constraints found in the provided list
     * @throws CloneNotSupportedException
     */
    private List<TableConstraint> extractUniqueConstraints(List<Table> sourceTables) throws CloneNotSupportedException {
        List<TableConstraint> constraints = new ArrayList<>();

        for(Table table: sourceTables){
            for(TableConstraint tc : table.getConstraints()){
                if(tc instanceof UniqueConstraint){
                    UniqueConstraint original = (UniqueConstraint) tc;
                    UniqueConstraint copy = (UniqueConstraint) original.clone();
                    List<Column> columns = new ArrayList<>(original.getColumns());
                    Collections.shuffle(columns);
                    copy.setColumns(columns);

                    constraints.add(copy);
                }
            }
        }

        Collections.shuffle(constraints);

        return constraints;
    }


    /**
     * Extract a list of referential constraints from a list of expected tables
     * For each ForeignKeyConstraint in the table list create a copy and shuffle the column order
     * to test how the assembler will order it
     *
     * It returns a list in the format defined at RelationalDBRepository.loadReferentialConstraints
     * @param sourceTables A list of fully filled tables
     * @return A list of referential constraints found in the provided list
     * @throws CloneNotSupportedException
     */
    private List<TableConstraint> extractFKConstraints(List<Table> sourceTables) throws CloneNotSupportedException {
        List<TableConstraint> constraints = new ArrayList<>();

        for(Table table: sourceTables){
            for(TableConstraint tc : table.getConstraints()){
                if(tc instanceof ForeignKeyConstraint){
                    ForeignKeyConstraint original = (ForeignKeyConstraint) tc;
                    ForeignKeyConstraint copy = (ForeignKeyConstraint) original.clone();
                    List<ReferentialIntegrityColumn> columns = new ArrayList<>(original.getColumns());
                    Collections.shuffle(columns);
                    copy.setColumns(columns);

                    constraints.add(copy);
                }
            }
        }

        Collections.shuffle(constraints);

        return constraints;
    }

    /**
     * Extract a list of check constraints from a list of expected tables
     * For each CheckConstraint in the table list create a copy of it
     *
     * It returns a list in the format defined at RelationalDBRepository.loadCheckConstraints
     * @param sourceTables A list of fully filled tables
     * @return A list of check constraints found in the provided list
     * @throws CloneNotSupportedException
     */
    private List<TableConstraint> extractCheckConstraints(List<Table> sourceTables) throws CloneNotSupportedException {
        List<TableConstraint> constraints = new ArrayList<>();

        for(Table table: sourceTables){
            for(TableConstraint tc : table.getConstraints()){
                if(tc instanceof CheckConstraint){
                    CheckConstraint original = (CheckConstraint) tc;
                    CheckConstraint copy = (CheckConstraint) original.clone();
                    constraints.add(copy);
                }
            }
        }

        Collections.shuffle(constraints);

        return constraints;
    }


    /**
     * Create a simple test table with 7 different columns, and 4 constraints : PK, FK, Unique, Check
     *
     * @param schema Schema of the table belongs
     * @param tableName Table name
     * @param sequentialId an ID to be used an identifier during the table and property generation; can called many times
     *                     it would be recommended to have a different id for each table
     * @return
     */
    private Table createFullTable(String schema, String tableName, int sequentialId){
        Table table = new Table(schema, tableName);

        table.addColumn(this.createNumericColumn(1, 25, 5, false));
        table.addColumn(this.createTextColumn(2, 30, false));
        table.addColumn(this.createTextColumn(3, 40, true));
        table.addColumn(this.createNumericColumn(4, 18, 2, false));
        table.addColumn(this.createTextColumn(5, 50, true));
        table.addColumn(this.createNumericColumn(6, 10, 3, false));
        table.addColumn(this.createNumericColumn(7, 28, 7, true));

        table.addConstraint(this.createUniqueConstraint(schema, tableName, true, sequentialId));
        table.addConstraint(this.createFKConstraint(schema, tableName, sequentialId));
        table.addConstraint(this.createUniqueConstraint(schema, tableName, false, 100+ sequentialId));
        table.addConstraint(new CheckConstraint(schema, tableName, "DummyChkConstraint" + sequentialId, "'FIELD01 > 78'"));

        return table;
    }

    private TableColumn createTextColumn(int orderId, long maxLength, boolean isNullable){
        TableColumn column = this.createTableColumn(orderId, "VARCHAR", isNullable);
        column.setTextMaxLength(maxLength);
        column.setDefaultValue("---<DEFAULT>---");

        return column;
    }

    private TableColumn createNumericColumn(int orderId, int numericPrecison, int numericScale, boolean isNullable){
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