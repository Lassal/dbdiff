package br.lassal.dbvcs.tatubola.relationaldb.repository;

import br.lassal.dbvcs.tatubola.relationaldb.model.*;

import java.util.*;

public class InMemoryTestRepository implements RelationalDBRepository{

    private class Schema{
        String name;
        List<Table> tables;
        List<View> views;
        List<Index> indexes;
        List<Trigger> triggers;
        List<Routine> routines;
    }

    private TableDummyBuilder tableBuilder;
    private ViewDummyBuilder viewBuilder;
    private IndexDummyBuilder indexBuilder;
    private TriggerDummyBuilder triggerBuilder;
    private RoutineDummyBuilder routineBuilder;
    private Map<String,Schema> schemas;

    public InMemoryTestRepository(){
        this.schemas = new HashMap<>();
        this.tableBuilder = new TableDummyBuilder();
        this.viewBuilder = new ViewDummyBuilder();
        this.indexBuilder = new IndexDummyBuilder();
        this.triggerBuilder = new TriggerDummyBuilder();
        this.routineBuilder = new RoutineDummyBuilder();
    }

    public void fillRepositoryWithSampleDBObjects(String ...schemas){
        for(String schema : schemas){
            this.schemas.put(schema, this.createSchemaDBObjects(schema));
        }
    }

    private Schema createSchemaDBObjects(String schemaName){
        Schema schema = new Schema();
        schema.name = schemaName;
        schema.tables = this.createSampleTables(schemaName);
        schema.views = this.createSampleViews(schemaName);
        schema.indexes = this.createSampleIndexes(schemaName);
        schema.triggers = this.createSampleTriggers(schemaName);
        schema.routines = this.createSampleRoutines(schemaName);

        return schema;
    }

    private List<Table> createSampleTables(String schema){
        List<Table> tables = new ArrayList<>();

        for(int i=1; i < 5; i++){
            tables.add(this.tableBuilder.createFullTable(schema,this.generateTableName(schema, i), i ));
        }

        return tables;
    }

    private String generateTableName(String schema, int id){
        return "TABLE_" + schema + "_" + id;
    }

    private List<View> createSampleViews(String schema){
        List<View> views = new ArrayList<>();

        for(int i=1; i < 3; i++){
            views.add(this.viewBuilder.createView(schema,"VIEW_" + schema + "_" + i, i ));
        }

        return views;
    }

    private List<Index> createSampleIndexes(String schema){
        List<Index> indexes = new ArrayList<>();

        for(int i=1; i < 5; i++){
            boolean unique = i % 2 == 1;
            indexes.add(this.indexBuilder.createTestIndex(schema, i, unique, true));
        }

        return indexes;
    }

    private List<Trigger> createSampleTriggers(String schema){
        List<Trigger> triggers = new ArrayList<>();

        for(int i=0; i < 6; i++){
            int idTable = (i / 2) + 1;
            triggers.add(this.triggerBuilder.createSampleTrigger(i+1, schema, this.generateTableName(schema, idTable)));
        }

        return triggers;
    }

    private List<Routine> createSampleRoutines(String schema){
        List<Routine> routines = new ArrayList<>();

        routines.add(this.routineBuilder.createProcedure(schema, "PROC_SAMPLE01_" + schema, 1));
        routines.add(this.routineBuilder.createProcedure(schema, "PROC_XPTO_" + schema, 2));
        routines.add(this.routineBuilder.createFunction(schema, "FUNC_CALC_SOMETHING" + schema, 3));

        return routines;
    }

    public Set<String> getSchemaNames(){
        return this.schemas.keySet();
    }

    @Override
    public Map<String, Table> loadTableColumns(String schema) {
        return null;
    }

    @Override
    public List<TableConstraint> loadUniqueConstraints(String schema) {
        return null;
    }

    @Override
    public List<TableConstraint> loadReferentialConstraints(String schema) {
        return null;
    }

    @Override
    public List<TableConstraint> loadCheckConstraints(String schema) {
        return null;
    }

    @Override
    public List<Index> loadIndexes(String schema) {
        return null;
    }

    @Override
    public List<View> loadViewDefinitions(String schema) {
        return null;
    }

    @Override
    public Map<String, List<Table>> loadViewTables(String schema) {
        return null;
    }

    @Override
    public Map<String, List<TableColumn>> loadViewColumns(String schema) {
        return null;
    }

    @Override
    public List<Trigger> loadTriggers(String schema) {
        return null;
    }

    @Override
    public List<Routine> loadRoutineDefinition(String schema) {
        return null;
    }

    @Override
    public List<RoutineParameter> loadRoutineParameters(String schema) {
        return null;
    }

    @Override
    public List<String> listSchemas() {
        return null;
    }

}
