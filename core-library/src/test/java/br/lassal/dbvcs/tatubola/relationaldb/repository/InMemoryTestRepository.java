package br.lassal.dbvcs.tatubola.relationaldb.repository;

import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InMemoryTestRepository implements RelationalDBRepository{

    private static Logger logger = LoggerFactory.getLogger(InMemoryTestRepository.class);

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

    @Override
    public Map<String, Table> loadTableColumns(String schemaName) {

        Map<String, Table> tableColumns = new HashMap<>();

        if(this.schemas.containsKey(schemaName)){
            Schema schema = this.schemas.get(schemaName);

            if(schema.tables != null){
                for(Table table: schema.tables){
                    Table copy = new Table(table.getSchema(), table.getName());
                    List<TableColumn> columns = new ArrayList<>(table.getColumns());
                    Collections.shuffle(columns);
                    columns.stream().forEach(c -> copy.addColumn(c));

                    tableColumns.put(copy.getTableID(), copy);
                }
            }
        }

        return tableColumns;
    }

    @Override
    public List<TableConstraint> loadUniqueConstraints(String schemaName) {

        List<TableConstraint> constraints = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)){
            Schema schema = this.schemas.get(schemaName);

            if(schema.tables != null){

                for(Table table: schema.tables){
                    for(TableConstraint tc : table.getConstraints()){
                        if(tc instanceof UniqueConstraint){
                            try{
                            UniqueConstraint original = (UniqueConstraint) tc;
                            UniqueConstraint copy = (UniqueConstraint) original.clone();
                            List<Column> columns = new ArrayList<>(original.getColumns());
                            Collections.shuffle(columns);
                            copy.setColumns(columns);

                            constraints.add(copy);
                            }
                            catch (CloneNotSupportedException e) {
                                logger.error("Error cloning UniqueConstraint", e);
                            }

                        }
                    }
                }
                Collections.shuffle(constraints);
            }
        }

        return constraints;
    }

    @Override
    public List<TableConstraint> loadReferentialConstraints(String schemaName) {
        List<TableConstraint> constraints = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.tables != null) {
                for(Table table: schema.tables){
                    for(TableConstraint tc : table.getConstraints()){
                        if(tc instanceof ForeignKeyConstraint){
                            try{
                                ForeignKeyConstraint original = (ForeignKeyConstraint) tc;
                                ForeignKeyConstraint copy = (ForeignKeyConstraint) original.clone();
                                List<ReferentialIntegrityColumn> columns = new ArrayList<>(original.getColumns());
                                Collections.shuffle(columns);
                                copy.setColumns(columns);

                                constraints.add(copy);
                            }
                            catch (CloneNotSupportedException e) {
                                logger.error("Error cloning ForeignKeyConstraint", e);
                            }
                        }
                    }
                }
                Collections.shuffle(constraints);
            }
        }

        return constraints;
    }

    @Override
    public List<TableConstraint> loadCheckConstraints(String schemaName) {
        List<TableConstraint> constraints = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.tables != null) {
                for(Table table: schema.tables){
                    for(TableConstraint tc : table.getConstraints()){
                        if(tc instanceof CheckConstraint){
                            try{
                                CheckConstraint original = (CheckConstraint) tc;
                                CheckConstraint copy = (CheckConstraint) original.clone();
                                constraints.add(copy);
                            } catch (CloneNotSupportedException e) {
                                logger.error("Error cloning CheckConstraint", e);
                            }
                        }
                    }
                }

                Collections.shuffle(constraints);
            }
        }

        return constraints;
    }

    @Override
    public List<Index> loadIndexes(String schemaName) {
        List<Index> indexes = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if(schema.indexes != null){
                indexes = schema.indexes;
            }
        }

        return indexes;
    }

    @Override
    public List<View> loadViewDefinitions(String schemaName) {
        List<View> viewDefinitions = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.views != null) {
                for (View original : schema.views) {
                    View copy = new View(original.getSchema(), original.getName());
                    copy.setInsertAllowed(original.isInsertAllowed());
                    copy.setUpdatedAllowed(original.isUpdatedAllowed());
                    copy.setViewDefinition(original.getViewDefinition());

                    viewDefinitions.add(copy);
                }
                Collections.shuffle(viewDefinitions);
            }
        }
        return viewDefinitions;
    }

    @Override
    public Map<String, List<Table>> loadViewTables(String schemaName) {
        Map<String, List<Table>> refTables = new HashMap<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.views != null) {
                for (View sourceView : schema.views) {
                    List<Table> tables = new ArrayList<>(sourceView.getReferencedTables());
                    Collections.shuffle(tables);

                    refTables.put(sourceView.getViewID(), tables);
                }
            }
        }
        return refTables;

    }

    @Override
    public Map<String, List<TableColumn>> loadViewColumns(String schemaName) {
        Map<String, List<TableColumn>> columns = new HashMap<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.views != null) {
                for (View sourceView : schema.views) {
                    List<TableColumn> tabCols = new ArrayList<>(sourceView.getColumns());
                    Collections.shuffle(tabCols);
                    columns.put(sourceView.getViewID(), tabCols);
                }
            }
        }
        return columns;
    }

    @Override
    public List<Trigger> loadTriggers(String schemaName) {
        List<Trigger> triggers = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if(schema.indexes != null){
                triggers = schema.triggers;
            }
        }

        return triggers;
    }

    @Override
    public List<Routine> loadRoutineDefinition(String schemaName) {
        List<Routine> routinesDefs = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.routines != null) {
                for (Routine original : schema.routines) {
                    Routine copy = new Routine(original.getSchema(), original.getName(), original.getRoutineType());
                    copy.setReturnParamater(original.getReturnParamater());
                    copy.setRoutineDefinition(original.getRoutineDefinition());

                    routinesDefs.add(copy);
                }

                Collections.shuffle(routinesDefs);
            }

        }
        return routinesDefs;
    }

    @Override
    public List<RoutineParameter> loadRoutineParameters(String schemaName) {
        List<RoutineParameter> parameters = new ArrayList<>();

        if(this.schemas.containsKey(schemaName)) {
            Schema schema = this.schemas.get(schemaName);

            if (schema.routines != null) {
                for (Routine sourceRoutine : schema.routines) {
                    parameters.addAll(sourceRoutine.getParameters());
                }
                Collections.shuffle(parameters);

            }
        }
        return parameters;

    }

    @Override
    public List<String> listSchemas() {
        return new ArrayList<>(this.schemas.keySet());
    }

}
