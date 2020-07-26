package br.lassal.dbvcs.tatubola.integration.fs;

import br.lassal.dbvcs.tatubola.fs.BaseDBModelFS;
import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.fs.DBModelFSException;
import br.lassal.dbvcs.tatubola.integration.util.FileSystemUtil;
import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BaseDBModelFSTest {

    private final String rootPath = "dsTests";

    @Before
    public void cleanUpOutputFiles() throws IOException {
        FileSystemUtil.deleteDir(new File(this.rootPath));
    }

    /**
     * Test the DBModelFS standard implementation for table serialization
     * Tests:
     *   - Table serialization to filesystem : {root path}/{TABLE SCHEMA}/Tables/TABLE_{table name}.yaml
     *   - Output path for table objects
     *   - Object deserialization from filesystem
     *
     * @throws IOException
     * @throws DBModelFSException
     */
    @Test
    public void testSerializeTable() throws IOException, DBModelFSException {
        DBModelFS dfs = new BaseDBModelFS(this.rootPath, new JacksonYamlSerializer());
        Table table = this.buildTable();

        dfs.save(table);

        Path expectedPath = Paths.get(this.rootPath, table.getSchema().toUpperCase(),"Tables", "TABLE_" + table.getName() + ".yaml");
        File outputFile = expectedPath.toFile();

        assertTrue(outputFile.exists());

        Table serializedTable =  dfs.loadFromFS(expectedPath, table.getClass());

        assertEquals(table, serializedTable);

    }

    /**
     * Test the DBModelFS standard implementation for view serialization
     * Tests:
     *   - view serialization to filesystem : {root path}/{VIEW SCHEMA}/Views/VIEW_{view name}.yaml
     *   - Output path for view objects
     *   - Object deserialization from filesystem
     *
     * @throws IOException
     * @throws DBModelFSException
     */
    @Test
    public void testSerializeView() throws IOException, DBModelFSException {
        DBModelFS dfs = new BaseDBModelFS(this.rootPath, new JacksonYamlSerializer());
        View view = this.buildView();

        dfs.save(view);

        Path expectedPath = Paths.get(this.rootPath, view.getSchema().toUpperCase(),"Views", "VIEW_" + view.getName() + ".yaml");
        File outputFile = expectedPath.toFile();

        assertTrue(outputFile.exists());

        View serializedView =  dfs.loadFromFS(expectedPath, view.getClass());

        assertEquals(view, serializedView);

    }

    /**
     * Test the DBModelFS standard implementation for index serialization
     * Tests:
     *   - Index serialization to filesystem : {root path}/{TABLE SCHEMA}/Tables/Indexes/TABLE_{table name}_INDEX_{index name}.yaml
     *   - Output path for index objects
     *   - Object deserialization from filesystem
     *
     * @throws IOException
     * @throws DBModelFSException
     */
    @Test
    public void testSerializeIndex() throws IOException, DBModelFSException {
        DBModelFS dfs = new BaseDBModelFS(this.rootPath, new JacksonYamlSerializer());
        Index index = this.buildIndex();

        dfs.save(index);

        Path expectedPath = Paths.get(this.rootPath, index.getSchema().toUpperCase(), "Tables/Indexes"
                , "TABLE_" + index.getAssociateTableName() + "_INDEX_" + index.getName() + ".yaml");
        File outputFile = expectedPath.toFile();

        assertTrue(outputFile.exists());

        Index serializedIndex = dfs.loadFromFS(expectedPath, Index.class);
        assertEquals(index, serializedIndex);
    }

    /**
     * Test the DBModelFS standard implementation for trigger serialization
     * Tests:
     *   - Index serialization to filesystem :
     *     {root path}/{TABLE SCHEMA}/Tables/Triggers/{TARGET OBJECT TYPE | OBJECT}_{target obj name}_Trigger_{trigger name}.yaml
     *   - Output path for trigger objects
     *   - Object deserialization from filesystem
     *
     * @throws IOException
     * @throws DBModelFSException
     */
    @Test
    public void testSerializeTrigger() throws IOException, DBModelFSException {
        DBModelFS dfs = new BaseDBModelFS(this.rootPath, new JacksonYamlSerializer());
        Trigger trigger = this.buildTrigger();

        dfs.save(trigger);

        Path expectedPath = Paths.get(this.rootPath, trigger.getSchema().toUpperCase(), "Tables/Triggers"
                , trigger.getTargetObjectType().toUpperCase() +  "_" + trigger.getTargetObjectName()
                        + "_TRIGGER_" + trigger.getName() + ".yaml");
        File outputFile = expectedPath.toFile();

        assertTrue(outputFile.exists());

        Trigger serializedTrigger = dfs.loadFromFS(expectedPath, Trigger.class);
        assertEquals(trigger, serializedTrigger);
    }

    /**
     * Test the DBModelFS standard implementation for view serialization
     * Tests:
     *   - view serialization to filesystem : {root path}/{ROUTINE SCHEMA}/Routines/{routine name}_{routine type}.yaml
     *   - Output path for routine objects
     *   - Object deserialization from filesystem
     *
     * @throws IOException
     * @throws DBModelFSException
     */
    @Test
    public void testSerializeRoutine() throws IOException, DBModelFSException {
        DBModelFS dfs = new BaseDBModelFS(this.rootPath, new JacksonYamlSerializer());
        Routine routine = this.buildRoutine();

        dfs.save(routine);

        Path expectedPath = Paths.get(this.rootPath, routine.getSchema().toUpperCase(),"Routines"
                , routine.getName() + "_" + routine.getRoutineType() + ".yaml");
        File outputFile = expectedPath.toFile();

        assertTrue(outputFile.exists());

        Routine serializedRoutine = dfs.loadFromFS(expectedPath, Routine.class);
        assertEquals(routine, serializedRoutine);

    }

    private Table buildTable(){
        Table table = new Table("schemaA", "TableA");
        table.addColumn(this.buildColumn1());
        table.addColumn(this.buildColumn2());

        UniqueConstraint constr1 = new UniqueConstraint(table.getSchema(), table.getName(), "PK_01", ConstraintType.PRIMARY_KEY);
        constr1.addColumn(new Column("Column1", 1));
        constr1.addColumn(new Column("Column2", 2));

        table.addConstraint(constr1);

        CheckConstraint constr2 = new CheckConstraint(table.getSchema(), table.getName(), "Check_something", "Column1 > 0");
        table.addConstraint(constr2);

        return table;
    }

    private TableColumn buildColumn1(){
        TableColumn col1 = new TableColumn("Column1");
        col1.setOrdinalPosition(1);
        col1.setNullable(false);
        col1.setDataType("NUMBER");
        col1.setDefaultValue("27");
        col1.setNumericPrecision(8);
        col1.setNumericScale(3);

        return col1;
    }

    private TableColumn buildColumn2(){
        TableColumn col2 = new TableColumn("Column2");
        col2.setOrdinalPosition(2);
        col2.setNullable(false);
        col2.setDataType("VARCHAR");
        col2.setDefaultValue("-- default value");
        col2.setTextMaxLength(50L);

        return col2;
    }

    private View buildView(){
        View view = new View("SchemaB", "ViewSample");
        view.setUpdatedAllowed(true);
        view.setInsertAllowed(true);
        view.setViewDefinition("SELECT\n" +
                "    NAME\n" +
                "  FROM\n" +
                "    ORDDATA.ORDDCM_CT_PRED_SET_TMP\n" +
                "  WHERE\n" +
                "    PSTYPE = 1\n" +
                "    AND SUPER IS NULL\n" +
                "    AND STATUS = 1 WITH READ ONLY");

        view.addColumn(this.buildColumn1());
        view.addColumn(this.buildColumn2());

        view.addTable("SchemaC", "OtherTable1");
        view.addTable("SchemaD", "OtherTable2");

        return view;
    }

    private Index buildIndex(){
        Index index = new Index("schemaA","PK_TableA", "schemaA", "TableA", "PRIMARY KEY", true);
        index.addColumn(new IndexColumn("Column1",1, ColumnOrder.ASC));
        index.addColumn(new IndexColumn("Column2",2, ColumnOrder.ASC));

        return index;
    }

    private Trigger buildTrigger(){
        Trigger trigger = new Trigger("TriggerSample");
        trigger.setTargetObjectSchema("schemaA");
        trigger.setTargetObjectName("TableA");
        trigger.setTargetObjectType("TABLE");
        trigger.setEvent("INSERT");
        trigger.setEventTiming("BEFORE");
        trigger.setExecutionOrder(1);
        trigger.setEventActionBody(" BEGIN\n" +
                "    add_job_history(\n" +
                "      :old.employee_id,\n" +
                "      :old.hire_date,\n" +
                "      sysdate,\n" +
                "      :old.job_id,\n" +
                "      :old.department_id\n" +
                "    );\n" +
                "  END;");

        return trigger;
    }

    private Routine buildRoutine(){
        Routine routine = new Routine("schemaF", "Routine01", RoutineType.FUNCTION);
        routine.setReturnParamater(new TypedColumn("", 0, "NUMBER"));
        routine.addParameter(new RoutineParameter("Param1", 1, "VARCHAR", ParameterMode.IN));
        routine.addParameter(new RoutineParameter("Param2", 2, "LONG", ParameterMode.IN));

        routine.setRoutineDefinition(" DECLARE customerLevel VARCHAR(20);\n" +
                "\n" +
                "    IF credit > 50000 THEN\n" +
                "\t\tSET customerLevel = 'PLATINUM';\n" +
                "    ELSEIF (credit >= 50000 AND \n" +
                "\t\t\tcredit <= 10000) THEN\n" +
                "        SET customerLevel = 'GOLD';\n" +
                "    ELSEIF credit < 10000 THEN\n" +
                "        SET customerLevel = 'SILVER';\n" +
                "    END IF;\n" +
                "\t-- return the customer level\n" +
                "\tRETURN (customerLevel);");

        return routine;
    }
}
