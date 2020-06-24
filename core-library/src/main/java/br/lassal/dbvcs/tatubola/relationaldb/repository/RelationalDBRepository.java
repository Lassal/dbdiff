package br.lassal.dbvcs.tatubola.relationaldb.repository;

import br.lassal.dbvcs.tatubola.relationaldb.model.*;

import java.util.List;
import java.util.Map;

public interface RelationalDBRepository {

    // Table
    Map<String, Table> loadTableColumns(String schema);
    List<TableConstraint> loadUniqueConstraints(String schema);
    List<TableConstraint> loadReferentialConstraints(String schema);
    List<TableConstraint> loadCheckConstraints(String schema);

    // Index
    List<Index> loadIndexes(String schema);

    // Views
    List<View> loadViewDefinitions(String schema);
    Map<String, List<Table>> loadViewTables(String schema);
    Map<String, List<TableColumn>> loadViewColumns(String schema);

    // Triggers
    List<Trigger> loadTriggers(String schema);

    // Routines
    List<Routine> loadRoutineDefinition(String schema);
    List<RoutineParameter> loadRoutineParameters(String schema);

    // General - Database
    List<String> listSchemas();
}
