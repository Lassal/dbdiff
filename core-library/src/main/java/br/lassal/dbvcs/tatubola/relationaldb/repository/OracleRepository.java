package br.lassal.dbvcs.tatubola.relationaldb.repository;

import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import br.lassal.dbvcs.tatubola.relationaldb.model.sort.TriggerComparator;
import com.github.vertical_blank.sqlformatter.SqlFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OracleRepository extends BaseRepository {

    private static Logger logger = LoggerFactory.getLogger(OracleRepository.class);

    public static final String RETURN_PARAMETER_NAME = "(--return--)";

    public OracleRepository(DataSource dataSource) {
        super(dataSource);
    }

    public List<String> listSchemas() {
        String sql = "SELECT USERNAME FROM SYS.ALL_USERS";


        List<String> schemas = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                schemas.add(rs.getString("USERNAME"));
            }

        } catch (Exception ex) {
            logger.error("Error listing schemas", ex);
        }

        return schemas;

    }


    public List<Table> listTables(String schema) {
        Map<String, Table> tableMap = this.loadTableColumns(schema);


        List<TableConstraint> constraints = this.loadUniqueConstraints(schema);
        constraints.addAll(this.loadReferentialConstraints(schema));
        constraints.addAll(this.loadCheckConstraints(schema));

        for (TableConstraint constraint : constraints) {
            if (tableMap.containsKey(constraint.getTableID())) {
                tableMap.get(constraint.getTableID()).addConstraint(constraint);
            }
        }

        tableMap.values().stream().forEach(Table::onAfterLoad);

        return tableMap.values().stream()
                .sorted(Comparator.comparing(Table::getTableID))
                .collect(Collectors.toList());
    }

    public Map<String, Table> loadTableColumns(String schema) {
        String sql =
                "SELECT T.OWNER , T.TABLE_NAME, C.COLUMN_NAME, C.COLUMN_ID " +
                        "     , C.DATA_TYPE, C.NULLABLE, C.CHAR_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.DATA_DEFAULT " +
                        "FROM SYS.ALL_TABLES T " +
                        " INNER JOIN SYS.ALL_TAB_COLUMNS C " +
                        "   ON T.OWNER = C.OWNER AND T.TABLE_NAME = C.TABLE_NAME ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE T.OWNER = '%s'", schema);
        }

        sql += "ORDER BY C.OWNER , C.TABLE_NAME , C.COLUMN_NAME";


        List<Table> tables = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            Table currentTable = null;
            // loop through the result set
            while (rs.next()) {

                String tableSchema = rs.getString("OWNER");
                String tableName = rs.getString("TABLE_NAME");

                if (currentTable == null ||
                        !currentTable.getSchema().equals(tableSchema) ||
                        !currentTable.getName().equals(tableName)) {

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

        String columnName = rs.getString("COLUMN_NAME");
        TableColumn column = new TableColumn(columnName);

        column.setOrdinalPosition(rs.getInt("COLUMN_ID"));
        column.setDataType(rs.getString("DATA_TYPE"));
        column.setNullable("Y".equals(rs.getString("NULLABLE")));

        Long textMaxLength = rs.getLong("CHAR_LENGTH");
        if (!rs.wasNull() && textMaxLength > 0) {
            column.setTextMaxLength(textMaxLength);
        }

        Integer precision = rs.getInt("DATA_PRECISION");
        if (!rs.wasNull() && precision > 0) {
            column.setNumericPrecision(precision);
        }

        Integer scale = rs.getInt("DATA_SCALE");
        if (!rs.wasNull()) {
            column.setNumericScale(scale);
        }


        //LOAD Long datatypes in the end;
        column.setDefaultValue(rs.getString("DATA_DEFAULT"));

        return column;

    }


    public List<TableConstraint> loadUniqueConstraints(String schema) {

        String sql = "SELECT K.OWNER, K.CONSTRAINT_NAME, K.CONSTRAINT_TYPE, K.TABLE_NAME, C.COLUMN_NAME , C.POSITION " +
                "FROM SYS.ALL_CONSTRAINTS K " +
                "  INNER JOIN SYS.ALL_CONS_COLUMNS C " +
                "    ON K.OWNER = C.OWNER AND K.CONSTRAINT_NAME = C.CONSTRAINT_NAME AND K.CONSTRAINT_TYPE IN ('P','U')";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE K.OWNER = '%s'", schema);
        }

        sql += "ORDER BY K.OWNER, K.CONSTRAINT_NAME , C.COLUMN_NAME";

        List<TableConstraint> constraints = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            UniqueConstraint currentConstraint = null;
            // loop through the result set
            while (rs.next()) {

                String constraintSchema = rs.getString("OWNER");
                String constraintName = rs.getString("CONSTRAINT_NAME");

                if (currentConstraint == null ||
                        !currentConstraint.getSchema().equals(constraintSchema) ||
                        !currentConstraint.getName().equals(constraintName)) {


                    currentConstraint = this.convertToUniqueConstraint(rs, constraintSchema, constraintName);
                    constraints.add(currentConstraint);
                }

                Column column = this.convertToUniqueConstraintColumn(rs);
                currentConstraint.addColumn(column);
            }

        } catch (Exception ex) {
            logger.error("Error loading PK and Unique constraints", ex);
        }

        return constraints;
    }

    private UniqueConstraint convertToUniqueConstraint(ResultSet rs, String constraintSchema, String constraintName) throws SQLException {
        ConstraintType constraintType = ConstraintType.fromOracle(rs.getString("CONSTRAINT_TYPE"));
        String constraintTable = rs.getString("TABLE_NAME");

        return new UniqueConstraint(constraintSchema, constraintTable, constraintName, constraintType);
    }

    private Column convertToUniqueConstraintColumn(ResultSet rs) throws SQLException {
        return new Column(rs.getString("COLUMN_NAME"), rs.getInt("POSITION"));
    }

    public List<TableConstraint> loadReferentialConstraints(String schema) {

        String sql = "SELECT K.CONSTRAINT_TYPE, K.OWNER, K.TABLE_NAME , K.CONSTRAINT_NAME " +
                "      ,T.OWNER AS REF_SCHEMA, T.TABLE_NAME AS REF_TABLE " +
                "      , S.COLUMN_NAME , S.POSITION, T.COLUMN_NAME AS REF_COLUMN, T.POSITION REF_POSITION " +
                "FROM SYS.ALL_CONSTRAINTS K " +
                " INNER JOIN SYS.ALL_CONS_COLUMNS S " +
                "   ON K.OWNER = S.OWNER AND K.CONSTRAINT_NAME = S.CONSTRAINT_NAME AND K.CONSTRAINT_TYPE = 'R' " +
                " INNER JOIN SYS.ALL_CONS_COLUMNS T " +
                "   ON K.R_OWNER = T.OWNER AND K.R_CONSTRAINT_NAME = T.CONSTRAINT_NAME AND S.POSITION = T.POSITION ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE K.OWNER = '%s'", schema);
        }

        sql += "ORDER BY K.OWNER, K.CONSTRAINT_NAME , S.COLUMN_NAME";

        List<TableConstraint> constraints = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            ForeignKeyConstraint currentConstraint = null;
            // loop through the result set
            while (rs.next()) {

                String constraintSchema = rs.getString("OWNER");
                String constraintName = rs.getString("CONSTRAINT_NAME");

                if (currentConstraint == null ||
                        !currentConstraint.getSchema().equals(constraintSchema) ||
                        !currentConstraint.getName().equals(constraintName)) {


                    currentConstraint = this.convertToFKConstraint(rs, constraintSchema, constraintName);
                    constraints.add(currentConstraint);
                }

                ReferentialIntegrityColumn column = this.convertToReferentialConstraintColumn(rs);
                currentConstraint.addColumn(column);
            }

        } catch (Exception ex) {
            logger.error("Error loading PK and Unique constraints", ex);
        }

        return constraints;
    }

    private ForeignKeyConstraint convertToFKConstraint(ResultSet rs, String constraintSchema, String constraintName) throws SQLException {
        String constraintTable = rs.getString("TABLE_NAME");

        return new ForeignKeyConstraint(constraintSchema, constraintTable, constraintName);
    }

    private ReferentialIntegrityColumn convertToReferentialConstraintColumn(ResultSet rs) throws SQLException {
        ReferentialIntegrityColumn column = new ReferentialIntegrityColumn(
                rs.getString("COLUMN_NAME"), rs.getInt("POSITION"));

        column.setReferencedSchemaName(rs.getString("REF_SCHEMA"));
        column.setReferencedTableName(rs.getString("REF_TABLE"));
        column.setReferencedTableColumnName(rs.getString("REF_COLUMN"));

        return column;

    }

    public List<TableConstraint> loadCheckConstraints(String schema) {
        String sql = "SELECT K.OWNER, K.CONSTRAINT_NAME, K.TABLE_NAME , K.SEARCH_CONDITION, K.SEARCH_CONDITION_VC " +
                "FROM SYS.ALL_CONSTRAINTS K " +
                "WHERE K.CONSTRAINT_TYPE = 'C' ";


        if (schema != null && !schema.isEmpty()) {
            sql += String.format("AND K.OWNER = '%s'", schema);
        }

        sql += "ORDER BY K.OWNER , K.CONSTRAINT_NAME ";

        List<TableConstraint> constraints = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                constraints.add(this.convertToCheckConstraint(rs));
            }

        } catch (Exception ex) {
            logger.error("Error loading check constraints", ex);
        }

        return constraints;
    }

    private CheckConstraint convertToCheckConstraint(ResultSet rs) throws SQLException {
        String checkClause = rs.getString("SEARCH_CONDITION");

        if (checkClause == null || checkClause.isEmpty()) {
            checkClause = rs.getString("SEARCH_CONDITION_VC");
        }

        if (checkClause != null) {
            checkClause = SqlFormatter.of("pl/sql").format(checkClause);
        }

        return new CheckConstraint(
                rs.getString("OWNER")
                , rs.getString("TABLE_NAME")
                , rs.getString("CONSTRAINT_NAME")
                , checkClause);

    }


    public List<Index> loadIndexes(String schema) {
        String sql = "SELECT IC.INDEX_OWNER, IC.INDEX_NAME, IC.TABLE_OWNER, IC.TABLE_NAME " +
                "     , I.INDEX_TYPE, I.UNIQUENESS " +
                "     , IC.COLUMN_NAME, IC.COLUMN_POSITION, IC.DESCEND " +
                "FROM SYS.ALL_IND_COLUMNS IC " +
                " INNER JOIN SYS.ALL_INDEXES I " +
                "  ON IC.INDEX_OWNER = I.OWNER AND IC.INDEX_NAME = I.INDEX_NAME ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE I.TABLE_OWNER = '%s'", schema);
        }

        sql += "ORDER BY IC.TABLE_OWNER, IC.TABLE_NAME, IC.INDEX_OWNER, IC.INDEX_NAME, IC.COLUMN_POSITION";

        List<Index> indexes = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            Index currentIndex = null;

            while (rs.next()) {

                String indexSchema = rs.getString("INDEX_OWNER");
                String indexName = rs.getString("INDEX_NAME");

                if (currentIndex == null ||
                        !currentIndex.getSchema().equals(indexSchema) ||
                        !currentIndex.getName().equals(indexName)) {

                    currentIndex = this.convertToIndex(indexSchema, indexName, rs);
                    indexes.add(currentIndex);
                }

                IndexColumn column = this.convertToIndexColumn(rs);
                currentIndex.addColumn(column);
            }

        } catch (Exception ex) {
            logger.error("Error loading triggers", ex);
        }

        return indexes;
    }

    private Index convertToIndex(String indexSchema, String indexName, ResultSet rs) throws SQLException {
        String indexType = rs.getString("INDEX_TYPE");
        String uniqueness = rs.getString("UNIQUENESS");
        boolean isUnique = uniqueness != null && uniqueness.equals("UNIQUE");
        String targetTableSchema = rs.getString("TABLE_OWNER");
        String targetTable = rs.getString("TABLE_NAME");

        return new Index(indexSchema, indexName, targetTableSchema, targetTable, indexType, isUnique);
    }

    private IndexColumn convertToIndexColumn(ResultSet rs) throws SQLException {
        String columnName = rs.getString("COLUMN_NAME");
        int ordinalPosition = rs.getInt("COLUMN_POSITION");
        String descend = rs.getString("DESCEND");
        ColumnOrder order = ColumnOrder.fromOracle(descend);

        return new IndexColumn(columnName, ordinalPosition, order);
    }

    public List<View> listViews(String schema) {
        List<View> views = this.loadViewDefinitions(schema);

        Map<String, View> mapViews = views.stream().collect(Collectors.toMap(View::getViewID, Function.identity()));
        Map<String, List<Table>> referencedTables = this.loadViewTables(schema);
        Map<String, List<TableColumn>> viewColumns = this.loadViewColumns(schema);

        for (Map.Entry<String, View> view : mapViews.entrySet()) {
            if (referencedTables.containsKey(view.getKey())) {
                List<Table> viewTables = referencedTables.get(view.getKey());

                if (!viewTables.isEmpty()) {
                    view.getValue().setReferencedTables(viewTables);
                }
            }

            if (viewColumns.containsKey(view.getKey())) {
                List<TableColumn> columns = viewColumns.get(view.getKey());

                if (!columns.isEmpty()) {
                    view.getValue().setColumns(columns);
                }
            }
        }

        return views;
    }

    public List<View> loadViewDefinitions(String schema) {
        String sql = "SELECT OWNER, VIEW_NAME , TEXT_VC , READ_ONLY, TEXT  FROM SYS.ALL_VIEWS ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE OWNER = '%s'", schema);
        }

        sql += "ORDER BY OWNER , VIEW_NAME ";

        List<View> views = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            // loop through the result set
            while (rs.next()) {

                View view = this.convertToView(rs);
                views.add(view);
            }
        } catch (SQLException | IOException ex) {
            logger.error("Error loading Oracle View definition", ex);
        }

        return views;

    }

    /**
     * The order of the loading is very import because the column TEXT is a stream column in Oracle (type = LONG)
     * so it can't be mixed with the reading of the other columns otherwise the stream is lost, but we believe that if
     * we read the field first it will take more time to run and most of the views will fit in 4K chars so we
     * read the first truncated field TEXT_VC and then if it is 4K we try to read the TEXT field.
     *
     * @param rs
     * @return
     * @throws SQLException
     * @throws IOException
     */
    private View convertToView(ResultSet rs) throws SQLException, IOException {
        // this order of reading is very important
        // see https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/Java-streams-in-JDBC.html#GUID-6A449885-5082-44B2-93B6-36060EEA7C15
        String viewDefinition = rs.getString("TEXT_VC");

        if (viewDefinition == null || viewDefinition.isEmpty() || viewDefinition.length() >= 4000) {
            viewDefinition = rs.getString("TEXT");
        }

        if (viewDefinition != null) {
            viewDefinition = SqlFormatter.of("pl/sql").format(viewDefinition);
        }

        String viewSchema = rs.getString("OWNER");
        String viewName = rs.getString("VIEW_NAME");
        String isUpdatable = rs.getString("READ_ONLY");
        // if the view is READ_ONLY it is not updatable
        boolean updatableView = "Y".equals(isUpdatable) ? false : true;

        View view = new View(viewSchema, viewName);
        view.setInsertAllowed(updatableView);
        view.setUpdatedAllowed(updatableView);
        view.setViewDefinition(viewDefinition);

        return view;
    }

    public Map<String, List<Table>> loadViewTables(String schema) {
        String sql = "SELECT OWNER, NAME AS VIEW_NAME, REFERENCED_OWNER , REFERENCED_NAME, REFERENCED_TYPE " +
                "FROM SYS.ALL_DEPENDENCIES " +
                "WHERE TYPE = 'VIEW' AND REFERENCED_TYPE IN ('TABLE', 'VIEW') ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("AND OWNER = '%s'", schema);
        }

        sql += "ORDER BY OWNER, VIEW_NAME, REFERENCED_OWNER , REFERENCED_NAME ";

        Map<String, List<Table>> refTables = new HashMap<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            String currentViewID = null;
            List<Table> viewTables = null;

            // loop through the result set
            while (rs.next()) {
                String viewSchema = rs.getString("OWNER");
                String viewName = rs.getString("VIEW_NAME");
                String viewID = View.createViewID(viewSchema, viewName);

                if (!viewID.equals(currentViewID)) {
                    currentViewID = viewID;

                    if (refTables.containsKey(viewID)) {
                        viewTables = refTables.get(viewID);
                    } else {
                        viewTables = new ArrayList<>();
                        refTables.put(viewID, viewTables);
                    }
                }

                viewTables.add(this.convertToViewReferenceTable(rs));
            }
        } catch (SQLException ex) {
            logger.error("Error loading Oracle View referenced tables", ex);
        }

        return refTables;

    }

    private Table convertToViewReferenceTable(ResultSet rs) throws SQLException {
        String tableSchema = rs.getString("REFERENCED_OWNER");
        String tableName = rs.getString("REFERENCED_NAME");

        return new Table(tableSchema, tableName, false);
    }

    public Map<String, List<TableColumn>> loadViewColumns(String schema) {
        String sql = "SELECT C.OWNER, C.TABLE_NAME, C.COLUMN_NAME, C.COLUMN_ID, C.DATA_TYPE " +
                "           ,C.NULLABLE, C.CHAR_LENGTH, C.DATA_PRECISION, C.DATA_SCALE, C.DATA_DEFAULT " +
                "FROM SYS.ALL_TAB_COLUMNS C " +
                " INNER JOIN SYS.ALL_VIEWS V " +
                "   ON C.OWNER = V.OWNER AND C.TABLE_NAME = V.VIEW_NAME ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE  V.OWNER = '%s'", schema);
        }

        sql += "ORDER BY C.OWNER, C.TABLE_NAME, C.COLUMN_NAME";

        Map<String, List<TableColumn>> columns = new HashMap<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            String currentViewID = null;
            List<TableColumn> viewColumns = null;

            // loop through the result set
            while (rs.next()) {
                String viewSchema = rs.getString("OWNER");
                String viewName = rs.getString("TABLE_NAME");
                String viewID = View.createViewID(viewSchema, viewName);

                if (!viewID.equals(currentViewID)) {
                    currentViewID = viewID;

                    if (columns.containsKey(viewID)) {
                        viewColumns = columns.get(viewID);
                    } else {
                        viewColumns = new ArrayList<>();
                        columns.put(viewID, viewColumns);
                    }
                }

                viewColumns.add(this.convertToTableColumn(rs));
            }
        } catch (SQLException ex) {
            logger.error("Error loading Oracle view columns", ex);
        }

        return columns;

    }

    public List<Trigger> loadTriggers(String schema) {
        String sql =
                "SELECT TABLE_OWNER, TABLE_NAME, BASE_OBJECT_TYPE, OWNER, TRIGGER_NAME " +
                        "      ,TRIGGERING_EVENT, TRIGGER_TYPE, TRIGGER_BODY " +
                        "FROM SYS.ALL_TRIGGERS ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("WHERE TABLE_OWNER = '%s'", schema);
        }

        sql += "ORDER BY TABLE_OWNER, TABLE_NAME, TRIGGERING_EVENT, TRIGGER_TYPE";

        List<Trigger> triggers = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {

                Trigger trigger = this.convertToTrigger(rs);
                triggers.add(trigger);
            }

        } catch (Exception ex) {
            logger.error("Error loading triggers", ex);
        }

        triggers = this.setTriggerExecutionOrder(triggers);
        Collections.sort(triggers, TriggerComparator.getSingleton());

        return triggers;
    }

    /**
     * Define the trigger execution order based on the hierarchical relationship with the others triggers.
     * Usually all triggers have executionOrder equals to 1 but in case a trigger must be executed after
     * another trigger it will have its execution number converted to
     * 10^(ChildLevel + 1)
     * <p>
     * Example:
     * Trigger A1 do not execute before or after other trigger : executionOrder = 1
     * Trigger B2 execute after Trigger A1 - Level 1           : executionOrder = 1+10^(1+1) = 100+1
     * Trigger C3 execute after Trigger B2 - Level 2           : executionOrder = 1+10^(1+2) = 1000+1
     *
     * @param triggers
     * @return List of ordered triggers
     */
    private List<Trigger> setTriggerExecutionOrder(List<Trigger> triggers) {
        Map<String, ParentChildNode> triggersExecOrder = this.getTriggerHierarchy();

        for (Trigger trigger : triggers) {
            if (triggersExecOrder.containsKey(trigger.getTriggerID())) {
                int hierarchyLevel = triggersExecOrder.get(trigger.getTriggerID()).getLevel();

                if (hierarchyLevel > 0) {
                    // set the magnitude level to 10 power level, starting from 10^2
                    int execOrderMagnitude = (int) Math.pow(10, hierarchyLevel + 1);
                    int execOrder = execOrderMagnitude + trigger.getExecutionOrder();
                    trigger.setExecutionOrder(execOrder);
                }

            }
        }

        return triggers;
    }

    private Trigger convertToTrigger(ResultSet rs) throws SQLException {
        Trigger trigger = new Trigger();
        trigger.setName(rs.getString("TRIGGER_NAME"));
        trigger.setTargetObjectSchema(rs.getString("TABLE_OWNER"));
        trigger.setTargetObjectName(rs.getString("TABLE_NAME"));
        //todo: ?? consider an enum here ??
        trigger.setTargetObjectType(rs.getString("BASE_OBJECT_TYPE"));
        trigger.setEvent(rs.getString("TRIGGERING_EVENT"));
        trigger.setEventTiming(rs.getString("TRIGGER_TYPE"));
        trigger.setExecutionOrder(1);

        trigger.setEventActionBody(SqlFormatter
                .of("pl/sql")
                .format(rs.getString("TRIGGER_BODY")));

        return trigger;
    }

    /**
     * Defines trigger hierarchy.
     * Each trigger has a sequence of execution, this sequence is defined by the type of event and timing of
     * the event. If there are many triggers to the same target object for the same event and timing it is not
     * possible to know in what order they will execute. But for triggers that were created using
     * FOLLOWS | PRECEDES clauses it is possible to say if this trigger will be executed before (PRECEDES) or
     * after (FOLLOWS) a related trigger.
     * <p>
     * This method identify these hierarchical relations and returns a dictionary for each trigger showing
     * a linked dependency between triggers. The @class ParentChildNode has a property "getLevel()" that return
     * the current level of the trigger with relation to its parents. For nodes in root level the level is 0,
     * each child level increase this number in 1.
     * Example:
     * Node 1 (root) : Level = 0
     * Node 2       : Level = 1
     * Node 3      : Level = 2
     *
     * @return
     */
    private Map<String, ParentChildNode> getTriggerHierarchy() {
        String sql = "SELECT * FROM SYS.ALL_TRIGGER_ORDERING ";

        Map<String, ParentChildNode> triggerHierarchy = new HashMap<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {

                String orderingType = rs.getString("ORDERING_TYPE");
                String triggerID = rs.getString("TRIGGER_OWNER") + "." + rs.getString("TRIGGER_NAME");
                String refTriggerID = rs.getString("REFERENCED_TRIGGER_OWNER") + "." + rs.getString("REFERENCED_TRIGGER_NAME");
                String parentID = orderingType.equals("FOLLOWS") ? refTriggerID : triggerID;
                String childID = orderingType.equals("FOLLOWS") ? triggerID : refTriggerID;

                ParentChildNode parent = null;

                if (triggerHierarchy.containsKey(parentID)) {
                    parent = triggerHierarchy.get(parentID);
                } else {
                    parent = new ParentChildNode(parentID);
                    triggerHierarchy.put(parent.getId(), parent);
                }

                ParentChildNode child = null;
                if (triggerHierarchy.containsKey(childID)) {
                    child = triggerHierarchy.get(childID);
                } else {
                    child = new ParentChildNode(childID);
                    triggerHierarchy.put(child.getId(), child);
                }

                child.setParent(parent);

            }

        } catch (Exception ex) {
            logger.error("Error loading Oracle triggers references", ex);
        }

        return triggerHierarchy;
    }

    public List<Routine> listRoutines(String schema) {
        List<Routine> routines = this.loadRoutineDefinition(schema);
        Map<String, Routine> routinesMap = routines.stream().collect(Collectors.toMap(Routine::getRoutineID, Function.identity()));

        Routine currentRoutine = null;
        for (RoutineParameter param : this.loadRoutineParameters(schema)) {

            if (currentRoutine == null || !param.getRoutineID().equals(currentRoutine.getRoutineID())) {
                currentRoutine = routinesMap.containsKey(param.getRoutineID()) ?
                        routinesMap.get(param.getRoutineID()) : null;
            }

            if (currentRoutine != null) {
                if (RoutineType.FUNCTION.equals(currentRoutine.getRoutineType())
                        && param.getOrdinalPosition() == 0) {
                    currentRoutine.setReturnParamater(param);
                } else {
                    currentRoutine.addParameter(param);
                }
            }
        }

        return routines;
    }

    public List<Routine> loadRoutineDefinition(String schema) {
        String sql =
                "SELECT OWNER, NAME, TYPE, LINE, TEXT " +
                        "FROM SYS.ALL_SOURCE " +
                        "WHERE TYPE IN ('PACKAGE', 'PROCEDURE', 'FUNCTION') ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("AND OWNER = '%s'", schema);
        }

        sql += "ORDER BY OWNER, NAME, LINE ";

        List<Routine> routines = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            Routine currentRoutine = null;
            StringBuilder routineBody = null;

            while (rs.next()) {

                String routineSchema = rs.getString("OWNER");
                String routineName = rs.getString("NAME");

                String routineID = Routine.getRoutineID(routineSchema, routineName);

                if (currentRoutine == null || !routineID.equals(currentRoutine.getRoutineID())) {

                    if (currentRoutine != null) {
                        String routineDefinition = SqlFormatter.of("pl/sql")
                                .format(routineBody.toString());
                        currentRoutine.setRoutineDefinition(routineDefinition);

                        routines.add(currentRoutine);
                    }

                    RoutineType routineType = RoutineType.fromOracle(rs.getString("TYPE"));
                    currentRoutine = new Routine(routineSchema, routineName, routineType);
                    routineBody = new StringBuilder();
                }

                String routineLine = rs.getString("TEXT");
                routineBody.append(routineLine + "\n");


            }
        } catch (SQLException ex) {
            logger.error("Error loading routine definitions", ex);
        }

        return routines;
    }


    public List<RoutineParameter> loadRoutineParameters(String schema) {
        String sql =
                "SELECT OWNER, PACKAGE_NAME, OBJECT_NAME, TRIM(OVERLOAD) OVERLOAD, " +
                        "  ARGUMENT_NAME, POSITION, DATA_TYPE , IN_OUT, DATA_LEVEL, " +
                        "  CHAR_LENGTH, DATA_PRECISION, DATA_SCALE " +
                        "FROM SYS.ALL_ARGUMENTS " +
                        "WHERE DATA_LEVEL = 0 ";

        if (schema != null && !schema.isEmpty()) {
            sql += String.format("AND OWNER = '%s'", schema);
        }

        sql += "ORDER BY OWNER, PACKAGE_NAME, OBJECT_NAME, NVL(OVERLOAD,1), POSITION ";

        List<RoutineParameter> parameters = new ArrayList<>();

        try (Connection conn = this.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

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
        String routineSchema = rs.getString("OWNER");
        String packageName = rs.getString("PACKAGE_NAME");
        String routineName = rs.getString("OBJECT_NAME");
        String overloadID = rs.getString("OVERLOAD");
        String paramName = rs.getString("ARGUMENT_NAME");
        String paramDataType = rs.getString("DATA_TYPE");
        int paramPosition = rs.getInt("POSITION");
        String paramModeLiteral = rs.getString("IN_OUT");
        int dataLevel = rs.getInt("DATA_LEVEL");
        ParameterMode paramMode = ParameterMode.fromOracle(paramModeLiteral);

        overloadID = overloadID == null ? "" : "(" + overloadID + ")";

        if ((paramName == null || paramName.isEmpty()) && dataLevel == 0) {
            paramName = OracleRepository.RETURN_PARAMETER_NAME;
        }

        if (packageName != null && !packageName.isEmpty()) {

            paramName = routineName + overloadID + "." + paramName;
            routineName = packageName;
        } else {
            //routineName = routineName + overloadID;
            paramName = overloadID + paramName;
        }

        RoutineParameter param = new RoutineParameter(paramName, paramPosition, paramDataType, paramMode);
        param.setRoutineSchema(routineSchema);
        param.setRoutineName(routineName);

        Long textMaxLength = rs.getLong("CHAR_LENGTH");
        if (!rs.wasNull() && textMaxLength > 0) {
            param.setTextMaxLength(textMaxLength);
        }

        Integer precision = rs.getInt("DATA_PRECISION");
        if (!rs.wasNull() && precision > 0) {
            param.setNumericPrecision(precision);
        }

        Integer scale = rs.getInt("DATA_SCALE");
        if (!rs.wasNull()) {
            param.setNumericScale(scale);
        }


        return param;
    }


}
