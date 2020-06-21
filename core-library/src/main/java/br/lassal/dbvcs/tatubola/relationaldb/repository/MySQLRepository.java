package br.lassal.dbvcs.tatubola.relationaldb.repository;

import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import com.github.vertical_blank.sqlformatter.SqlFormatter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MySQLRepository implements RelationalDBRepository{


    private static final String SCHEMA = "TABLE_SCHEMA";
    private static final String TABLE_NAME = "TABLE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";

    private static Logger logger = LoggerFactory.getLogger(MySQLRepository.class);

    private static HikariDataSource ds;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl( "jdbc:mysql://localhost:9501/classicmodels" );
        config.setUsername( "local-admin" );
        config.setPassword( "db12345");
        config.setMinimumIdle(4);
        config.setMaximumPoolSize(16);
      //  config.addDataSourceProperty( "cachePrepStmts" , "true" );
      //  config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
      //  config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource( config );
    }

    private boolean useLocalConnection;

    public MySQLRepository(boolean useLocalConnection){
        this.useLocalConnection = useLocalConnection;
    }

    public MySQLRepository(){
        this(false);
    }

    private Connection getConnection() throws SQLException {
        long start = System.nanoTime();
        Connection conn = this.useLocalConnection ? this.getConnectionLocal() : this.getConnectionConnectionPool();
        System.out.println(String.format("---GET CONNECTION: [%s] mic %f | %d %n", Thread.currentThread().getName(), ((System.nanoTime()-start)/1000.00), System.nanoTime()));

        return conn;
    }

    private Connection getConnectionConnectionPool() throws SQLException{
        return ds.getConnection();
    }

    private Connection getConnectionLocal() throws SQLException {
        String url       = "jdbc:mysql://localhost:9501/classicmodels";
        String username = "local-admin";
        String password = "db12345";

        return DriverManager.getConnection(url,username,password);
    }

    public List<Table> listTables(String schema){
        Map<String, Table> tableMap = this.loadTableColumns(schema);

        List<TableConstraint> otherConstraints = this.loadPKFKUniqueConstraints(schema);
        for (TableConstraint constraint: otherConstraints) {
            if(tableMap.containsKey(constraint.getTableID())){
                tableMap.get(constraint.getTableID()).addConstraint(constraint);
            }
        }


        List<TableConstraint> checkConstraints = this.loadCheckConstraints(schema);
        for (TableConstraint constraint: checkConstraints) {
            if(tableMap.containsKey(constraint.getTableID())){
                tableMap.get(constraint.getTableID()).addConstraint(constraint);
            }
        }

        tableMap.values().stream().forEach(Table::onAfterLoad);

        return tableMap.values().stream()
                .sorted(Comparator.comparing(Table::getTableID))
                .collect(Collectors.toList());
    }

    public Map<String, Table> loadTableColumns(String schema){
        String sql = "SELECT C.* FROM information_schema.TABLES T " +
                     "  INNER JOIN information_schema.COLUMNS C " +
                     "   ON T.TABLE_SCHEMA = C.TABLE_SCHEMA AND T.TABLE_NAME = C.TABLE_NAME AND T.TABLE_TYPE = 'BASE TABLE' ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE T.TABLE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY C.TABLE_SCHEMA , C.TABLE_NAME , C.COLUMN_NAME";


        List<Table> tables = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            Table currentTable = null;
            // loop through the result set
            while (rs.next()) {

                String tableSchema = rs.getString(MySQLRepository.SCHEMA);
                String tableName = rs.getString(MySQLRepository.TABLE_NAME);

                if(     currentTable == null ||
                        !currentTable.getSchema().equals(tableSchema) ||
                        !currentTable.getName().equals(tableName)){

                    currentTable = new Table(tableSchema, tableName);
                    tables.add(currentTable);
                }

                TableColumn column = this.convertToTableColumn(rs);
                currentTable.addColumn(column);
            }
        } catch (SQLException ex) {
            logger.error("Error loading table columns", ex);
        }

        return tables.stream().collect(Collectors.toMap(Table::getTableID, Function.identity()));

    }

    private TableColumn convertToTableColumn(ResultSet rs) throws SQLException {

        String columnName = rs.getString(MySQLRepository.COLUMN_NAME);
        TableColumn column = new TableColumn(columnName);

        column.setOrdinalPosition(rs.getInt("ORDINAL_POSITION"));
        column.setDataType(rs.getString("DATA_TYPE"));
        column.setNullable("YES".equals(rs.getString("IS_NULLABLE")));

        Long textMaxLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
        if(!rs.wasNull() && textMaxLength > 0){
            column.setTextMaxLength(textMaxLength);
        }

        Integer precision = rs.getInt("NUMERIC_PRECISION");
        precision = rs.wasNull() ? rs.getInt("DATETIME_PRECISION") : precision;
        if(!rs.wasNull() && precision > 0){
            column.setNumericPrecision(precision);
        }

        Integer scale = rs.getInt("NUMERIC_SCALE");
        if(!rs.wasNull()){
            column.setNumericScale(scale);
        }

        column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));

        return column;

    }

    public List<TableConstraint> loadUniqueConstraints(String schema){
        return this.loadPKFKUniqueConstraints(schema);
    }

    public List<TableConstraint> loadReferentialConstraints(String schema){
        return null;
    }

    public List<TableConstraint> loadPKFKUniqueConstraints(String schema){

        String sql = "SELECT TC.CONSTRAINT_TYPE, TC.ENFORCED, CC.* " +
                     "FROM information_schema.TABLE_CONSTRAINTS TC " +
                     "  INNER JOIN information_schema.KEY_COLUMN_USAGE CC " +
                     "    ON TC.CONSTRAINT_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME  = CC.CONSTRAINT_NAME " +
                     "       AND TC.TABLE_NAME = CC.TABLE_NAME AND TC.CONSTRAINT_TYPE <> 'CHECK' ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE TC.TABLE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY TABLE_SCHEMA , TABLE_NAME , CONSTRAINT_NAME, ORDINAL_POSITION";

        List<TableConstraint> constraints = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            TableConstraint constraint = null;

            do{
                constraint = this.buildConstraint(rs);

                if(constraint != null){
                    constraints.add(constraint);
                }
            }while(constraint != null);

        } catch (Exception ex) {
            logger.error("Error loading PK, FK and Unique constraints", ex);
        }

        return constraints;
    }

    private TableConstraint buildConstraint(ResultSet rs) throws SQLException {
        TableConstraint constraint = null;

        if(rs.next()){
            String tableSchema = rs.getString(MySQLRepository.SCHEMA);
            String tableName = rs.getString(MySQLRepository.TABLE_NAME);
            String constraintName = rs.getString("CONSTRAINT_NAME");
            ConstraintType type = ConstraintType.fromMySQL(rs.getString("CONSTRAINT_TYPE"));

            if(ConstraintType.FOREIGN_KEY.equals(type)){
                constraint = new ForeignKeyConstraint(tableSchema, tableName, constraintName);

            }
            else{
                constraint = new UniqueConstraint(tableSchema, tableName, constraintName, type);
            }

            this.addConstraintColumn(constraint, rs);

            while(rs.next()){
                String currTableSchema = rs.getString(MySQLRepository.SCHEMA);
                String currTableName = rs.getString(MySQLRepository.TABLE_NAME);
                String currConstraintName = rs.getString("CONSTRAINT_NAME");

                if(currTableSchema.equals(tableSchema) && currTableName.equals(tableName)
                        && currConstraintName.equals(constraintName)){
                    this.addConstraintColumn(constraint, rs);
                }
                else{
                    rs.previous();
                    break;
                }
            }
        }
        return constraint;
    }

    private void addConstraintColumn(TableConstraint constraint, ResultSet rs) throws SQLException {
        if(constraint instanceof UniqueConstraint){
            UniqueConstraint uniqueConstr = (UniqueConstraint) constraint;
            uniqueConstr.addColumn(this.convertToConstraintColumn(rs));
        }
        else if(constraint instanceof ForeignKeyConstraint){
            ((ForeignKeyConstraint) constraint).addColumn(this.convertToReferentialIntegrityColumn(rs));
        }

    }

    private ReferentialIntegrityColumn convertToReferentialIntegrityColumn(ResultSet rs) throws SQLException {
        ReferentialIntegrityColumn column =
                new ReferentialIntegrityColumn(rs.getString(MySQLRepository.COLUMN_NAME), rs.getInt("ORDINAL_POSITION"));

        column.setReferencedSchemaName(rs.getString("REFERENCED_TABLE_SCHEMA"));
        column.setReferencedTableName(rs.getString("REFERENCED_TABLE_NAME"));
        column.setReferencedTableColumnName(rs.getString("REFERENCED_COLUMN_NAME"));
        return column;
    }

    private Column convertToConstraintColumn(ResultSet rs) throws SQLException {
        return new Column(rs.getString("COLUMN_NAME"), rs.getInt("ORDINAL_POSITION"));
    }

    public List<TableConstraint> loadCheckConstraints(String schema){
        String sql = "SELECT CC.CHECK_CLAUSE, TC.* " +
                     "FROM information_schema.TABLE_CONSTRAINTS TC " +
                     "  INNER JOIN information_schema.CHECK_CONSTRAINTS CC " +
                     "   ON TC.CONSTRAINT_SCHEMA = CC.CONSTRAINT_SCHEMA " +
                     "      AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME " +
                     "      AND TC.CONSTRAINT_TYPE = 'CHECK' ";



        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE TC.TABLE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY TABLE_SCHEMA , TABLE_NAME , CONSTRAINT_NAME";

        List<TableConstraint> constraints = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            while(rs.next()){
                String checkClause = SqlFormatter.of("sql").format(rs.getString("CHECK_CLAUSE"));
                CheckConstraint constraint =
                        new CheckConstraint(rs.getString(MySQLRepository.SCHEMA),rs.getString(MySQLRepository.TABLE_NAME)
                                          , rs.getString("CONSTRAINT_NAME"), checkClause);
                constraints.add(constraint);
            }

        } catch (Exception ex) {
            logger.error("Error loading check constraints",ex);
        }

        return constraints;
    }

    public List<Trigger> loadTriggers(String schema){
        String sql = "SELECT * FROM information_schema.TRIGGERS T ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE T.EVENT_OBJECT_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY TRIGGER_SCHEMA, TRIGGER_NAME";

        List<Trigger> triggers = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            while(rs.next()){

                Trigger trigger = this.convertToTrigger(rs);
                triggers.add(trigger);
            }

        } catch (Exception ex) {
            logger.error("Error loading triggers",ex);
        }


        /*
        TRIGGER_NAME
        EVENT_MANIPULATION
        EVENT_OBJECT_SCHEMA
        EVENT_OBJECT_TABLE
        ACTION_ORDER : EXECUTION_ORDER
        ACTION_TIMING
        ACTION_ORIENTATION
        ACTION_STATEMENT
         */

        return triggers;
    }

    private Trigger convertToTrigger(ResultSet rs) throws SQLException {
        Trigger trigger = new Trigger(rs.getString("TRIGGER_NAME"));
        trigger.setTargetObjectSchema(rs.getString("EVENT_OBJECT_SCHEMA"));
        trigger.setTargetObjectName(rs.getString("EVENT_OBJECT_TABLE"));
        //todo: ?? consider an enum here ??
        trigger.setTargetObjectType("TABLE");
        trigger.setEvent(rs.getString("EVENT_MANIPULATION") + " on " + rs.getString("ACTION_ORIENTATION"));
        trigger.setEventTiming(rs.getString("ACTION_TIMING"));
        trigger.setExecutionOrder(rs.getInt("ACTION_ORDER"));
        trigger.setEventActionBody(SqlFormatter
                .of("sql")
                .format(rs.getString("ACTION_STATEMENT")));

        return trigger;
    }

    public List<Routine> listRoutines(String schema){
        List<Routine> routines = this.loadRoutineDefinition(schema);
        Map<String, Routine> routinesMap = routines.stream().collect(Collectors.toMap(Routine::getRoutineID, Function.identity()));

        Routine currentRoutine = null;
        for(RoutineParameter param : this.loadRoutineParameters(schema)){

            if(currentRoutine == null || !param.getRoutineID().equals(currentRoutine.getRoutineID())){
                currentRoutine = routinesMap.containsKey(param.getRoutineID()) ?
                           routinesMap.get(param.getRoutineID()) : null;
            }

            if(currentRoutine != null){
                if(param.getOrdinalPosition() == 0){
                    currentRoutine.setReturnParamater(param);
                }
                else{
                    currentRoutine.addParameter(param);
                }
            }
        }

        return routines;
    }

    public List<Routine> loadRoutineDefinition(String schema){
        String sql =
                "SELECT ROUTINE_SCHEMA, ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION " +
                        "FROM information_schema.ROUTINES " ;

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE ROUTINE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME ";

        List<Routine> routines = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            // loop through the result set
            while (rs.next()) {

                Routine routine = this.convertToRoutine(rs);
                routines.add(routine);

            }
        } catch (SQLException ex) {
            logger.error("Error loading routine definitions", ex);
        }

       return routines;
    }

    private Routine convertToRoutine(ResultSet rs) throws SQLException {
        String routineSchema = rs.getString("ROUTINE_SCHEMA");
        String routineName = rs.getString("ROUTINE_NAME");
        RoutineType routineType = RoutineType.fromMySQL(rs.getString("ROUTINE_TYPE"));

        Routine routine = new Routine(routineSchema, routineName, routineType);
        String routineDefinition = SqlFormatter.of("sql").format(rs.getString("ROUTINE_DEFINITION"));
        routine.setRoutineDefinition(routineDefinition);

        return routine;
    }

    public List<RoutineParameter> loadRoutineParameters(String schema){
        String sql =
                "SELECT P.SPECIFIC_SCHEMA, P.SPECIFIC_NAME, P.ROUTINE_TYPE " +
                        "     , P.PARAMETER_NAME, P.DATA_TYPE, P.ORDINAL_POSITION, P.PARAMETER_MODE " +
                        "     , P.CHARACTER_MAXIMUM_LENGTH, P.NUMERIC_PRECISION, P.NUMERIC_SCALE, P.DATETIME_PRECISION " +
                        "FROM information_schema.PARAMETERS P ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE SPECIFIC_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY P.SPECIFIC_SCHEMA, P.SPECIFIC_NAME, P.ORDINAL_POSITION ";

        List<RoutineParameter> parameters = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            // loop through the result set
            while (rs.next()) {

                RoutineParameter param = this.convertToRoutineParameter(rs);
                parameters.add(param);

            }
        } catch (SQLException ex) {
            logger.error("Error loading routine paramaters", ex);
        }

        return parameters;
    }

    private RoutineParameter convertToRoutineParameter(ResultSet rs) throws SQLException {
        String routineSchema = rs.getString("SPECIFIC_SCHEMA");
        String routineName = rs.getString("SPECIFIC_NAME");
        String paramName = rs.getString("PARAMETER_NAME");
        String paramDataType = rs.getString("DATA_TYPE");
        int paramPosition = rs.getInt("ORDINAL_POSITION");
        String paramModeLiteral = rs.getString("PARAMETER_MODE");
        ParameterMode paramMode =  paramModeLiteral == null ? null : ParameterMode.valueOf(paramModeLiteral);

        RoutineParameter param = new RoutineParameter(paramName, paramPosition, paramDataType, paramMode);
        param.setRoutineSchema(routineSchema);
        param.setRoutineName(routineName);


        Long textMaxLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
        if(!rs.wasNull() && textMaxLength > 0){
            param.setTextMaxLength(textMaxLength);
        }

        Integer precision = rs.getInt("NUMERIC_PRECISION");
        precision = rs.wasNull() ? rs.getInt("DATETIME_PRECISION") : precision;
        if(!rs.wasNull() && precision > 0){
            param.setNumericPrecision(precision);
        }

        Integer scale = rs.getInt("NUMERIC_SCALE");
        if(!rs.wasNull()){
            param.setNumericScale(scale);
        }

        return param;
    }

    public List<Index> loadIndexes(String schema){
        String sql = "SELECT * FROM information_schema.STATISTICS I ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE I.TABLE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY TABLE_SCHEMA , TABLE_NAME , INDEX_SCHEMA , INDEX_NAME, SEQ_IN_INDEX";

        List<Index> indexes = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            Index currentIndex = null;

            while(rs.next()){

                String tableSchema = rs.getString("TABLE_SCHEMA");
                String tableName = rs.getString("TABLE_NAME");
                String indexSchema = rs.getString("INDEX_SCHEMA");
                String indexName = rs.getString("INDEX_NAME");

                if(     currentIndex == null ||
                        !currentIndex.getAssociateTableSchema().equals(tableSchema) ||
                        !currentIndex.getAssociateTableName().equals(tableName) ||
                        !currentIndex.getSchema().equals(indexSchema) ||
                        !currentIndex.getName().equals(indexName)){

                    currentIndex = this.convertToIndex(indexSchema, indexName, tableSchema, tableName, rs);
                    indexes.add(currentIndex);
                }

                IndexColumn column = this.convertToIndexColumn(rs);
                currentIndex.addColumn(column);
            }

        } catch (Exception ex) {
            logger.error("Error loading triggers",ex);
        }

        return indexes;
    }

    private Index convertToIndex(String indexSchema, String indexName, String tableSchema, String tableName, ResultSet rs) throws SQLException {
        String indexType = rs.getString("INDEX_TYPE");
        boolean isUnique = !rs.getBoolean("NON_UNIQUE");

        return new Index(indexSchema, indexName, tableSchema, tableName, indexType, isUnique);
    }

    private IndexColumn convertToIndexColumn(ResultSet rs) throws SQLException {
         String columnName = rs.getString("COLUMN_NAME");
         int ordinalPosition = rs.getInt("SEQ_IN_INDEX");
         String collation = rs.getString("COLLATION");
         ColumnOrder order  = ColumnOrder.fromMySQL(collation);

         return new IndexColumn(columnName, ordinalPosition, order);
    }

    public List<View> listViews(String schema){
        List<View> views = this.loadViewDefinitions(schema);

        Map<String, View> mapViews = views.stream().collect(Collectors.toMap(View::getViewID, Function.identity()));
        Map<String, List<Table>> referencedTables = this.loadViewTables(schema);
        Map<String, List<TableColumn>> viewColumns = this.loadViewColumns(schema);

        for(Map.Entry<String,View> view:mapViews.entrySet()){
            if(referencedTables.containsKey(view.getKey())){
                List<Table> viewTables =  referencedTables.get(view.getKey());

                if(viewTables.size() > 0){
                    view.getValue().setReferencedTables(viewTables);
                }
            }

            if(viewColumns.containsKey(view.getKey())){
                List<TableColumn> columns = viewColumns.get(view.getKey());

                if(columns.size() > 0){
                    view.getValue().setColumns(columns);
                }
            }
        }

        return views;
    }

    public List<View> loadViewDefinitions(String schema){
        String sql = "SELECT * FROM information_schema.VIEWS ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE TABLE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY TABLE_SCHEMA , TABLE_NAME ";

        List<View> views = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            // loop through the result set
            while (rs.next()) {

                View view = this.convertToView(rs);
                views.add(view);
            }
        } catch (SQLException ex) {
            logger.error("Error loading MySQL view definitions", ex);
        }

        return views;

    }

    private View convertToView(ResultSet rs) throws SQLException {
        String viewSchema = rs.getString("TABLE_SCHEMA");
        String viewName = rs.getString("TABLE_NAME");
        String isUpdatable = rs.getString("IS_UPDATABLE");
        String viewDefinition = SqlFormatter.of("sql").format(rs.getString("VIEW_DEFINITION"));
        boolean updatableView = "YES".equals(isUpdatable) ? true : false;


        View view = new View(viewSchema, viewName);
        view.setInsertAllowed(updatableView);
        view.setUpdatedAllowed(updatableView);
        view.setViewDefinition(viewDefinition);

        return view;
    }

    public Map<String,List<Table>> loadViewTables(String schema){
        String sql = "SELECT * FROM information_schema.VIEW_TABLE_USAGE ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE VIEW_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY VIEW_SCHEMA, VIEW_NAME, TABLE_SCHEMA , TABLE_NAME ";

        Map<String,List<Table>> refTables = new HashMap<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            String currentViewID = null;
            List<Table> viewTables = null;

            // loop through the result set
            while (rs.next()) {
                String viewSchema = rs.getString("VIEW_SCHEMA");
                String viewName = rs.getString("VIEW_NAME");
                String viewID = View.createViewID(viewSchema, viewName);

                if(!viewID.equals(currentViewID)){
                    currentViewID = viewID;

                    if(refTables.containsKey(viewID)){
                        viewTables = refTables.get(viewID);
                    }
                    else{
                        viewTables = new ArrayList<>();
                        refTables.put(viewID, viewTables);
                    }
                }

                viewTables.add(this.convertToViewReferenceTable(rs));
            }
        } catch (SQLException ex) {
            logger.error("Error loading MySQL view table references", ex);
        }

        return refTables;

    }

    private Table convertToViewReferenceTable(ResultSet rs) throws SQLException {
        String tableSchema = rs.getString("TABLE_SCHEMA");
        String tableName = rs.getString("TABLE_NAME");

        return new Table(tableSchema, tableName, false);
    }

    public Map<String, List<TableColumn>> loadViewColumns(String schema){
        String sql = "SELECT C.* " +
                     "FROM information_schema.VIEWS V " +
                     " INNER JOIN information_schema.COLUMNS C " +
                     "   ON V.TABLE_SCHEMA = C.TABLE_SCHEMA  AND V.TABLE_NAME = C.TABLE_NAME ";

        if(schema !=null && !schema.isEmpty()) {
            sql += String.format("WHERE  V.TABLE_SCHEMA = '%s'", schema);
        }

        sql += "ORDER BY C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME";

        Map<String,List<TableColumn>> columns = new HashMap<>();

        try (Connection conn = this.getConnection();
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            String currentViewID = null;
            List<TableColumn> viewColumns = null;

            // loop through the result set
            while (rs.next()) {
                String viewSchema = rs.getString("TABLE_SCHEMA");
                String viewName = rs.getString("TABLE_NAME");
                String viewID = View.createViewID(viewSchema, viewName);

                if(!viewID.equals(currentViewID)){
                    currentViewID = viewID;

                    if(columns.containsKey(viewID)){
                        viewColumns = columns.get(viewID);
                    }
                    else{
                        viewColumns = new ArrayList<>();
                        columns.put(viewID, viewColumns);
                    }
                }

                viewColumns.add(this.convertToTableColumn(rs));
            }
        } catch (SQLException ex) {
            logger.error("Error loading table columns", ex);
        }

        return columns;

    }
}
