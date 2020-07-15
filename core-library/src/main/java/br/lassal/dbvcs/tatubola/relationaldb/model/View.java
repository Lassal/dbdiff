package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.text.SqlNormalizer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@JsonPropertyOrder({"schema","name","updatedAllowed","insertAllowed","columns", "referencedTables", "viewDefinition"})
@JsonIgnoreProperties(ignoreUnknown = true, value = {"viewID"})
public class View implements DatabaseModelEntity {

    public static String createViewID(String viewSchema, String viewName) {
        return viewSchema + "." + viewName;
    }

    private String schema;
    private String name;
    private boolean updatedAllowed;
    private boolean insertAllowed;
    private String viewDefinition;
    private List<TableColumn> columns;
    private List<Table> referencedTables;


    public View() {
    }

    public View(String schema, String name) {
        this.schema = schema;
        this.name = name;
        this.setColumns(new ArrayList<>());
        this.setReferencedTables(new ArrayList<>());
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void tidyUpProperties(SqlNormalizer normalizer) {
        //throw new UnsupportedOperationException();
    }

    public boolean isUpdatedAllowed() {
        return updatedAllowed;
    }

    public void setUpdatedAllowed(boolean updatedAllowed) {
        this.updatedAllowed = updatedAllowed;
    }

    public boolean isInsertAllowed() {
        return insertAllowed;
    }

    public void setInsertAllowed(boolean insertAllowed) {
        this.insertAllowed = insertAllowed;
    }

    public String getViewDefinition() {
        return viewDefinition;
    }

    public void setViewDefinition(String viewDefinition) {
        this.viewDefinition = viewDefinition;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<TableColumn> columns) {
        this.columns = columns;
    }

    public List<Table> getReferencedTables() {
        return referencedTables;
    }

    public void setReferencedTables(List<Table> referencedTables) {
        this.referencedTables = referencedTables;
    }

    public void addTable(String tableSchema, String tableName) {
        Table table = new Table(tableSchema, tableName, false);
        this.referencedTables.add(table);
    }

    public void addColumn(TableColumn column) {
        this.columns.add(column);
    }

    public String getViewID() {
        return View.createViewID(this.schema, this.name);
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof View) {
            View other = (View) obj;
            isEqual = true;

            isEqual &= this.schema.equals(other.schema);
            isEqual &= this.name.equals(other.name);
            isEqual &= this.isInsertAllowed() == other.isInsertAllowed();
            isEqual &= this.isUpdatedAllowed() == other.isUpdatedAllowed();
            isEqual &= this.viewDefinition.equals(other.viewDefinition);

            isEqual &= this.getColumns().size() == other.getColumns().size();

            if (isEqual) {
                Map<String, TableColumn> thisColumns = this.columns.stream()
                        .collect(Collectors.toMap(TableColumn::getName, Function.identity()));

                for (TableColumn c : other.getColumns()) {
                    isEqual &= thisColumns.containsKey(c.getName());

                    if (isEqual) {
                        isEqual &= thisColumns.get(c.getName()).equals(c);
                    }
                }

            }

        }

        return isEqual;

    }

    @Override
    public String toString() {
        StringBuilder view = new StringBuilder();

        view.append(String.format("Schema: %s | View: %s%n", this.schema, this.name));
        view.append(String.format("InsertedAllowed: %s | UpdatedAllowed: %s %n", this.insertAllowed, this.updatedAllowed));

        view.append("  ::Columns\n");
        for (TableColumn column : this.columns) {
            view.append(String.format("    %s%n", column));
        }

        view.append("  ::RefTables & Views\n");
        for (Table refTable : this.referencedTables) {
            view.append(String.format("    %s%n", refTable.getTableID()));
        }
        view.append("\n----- VIEW DEFINITION -----\n");
        view.append(this.viewDefinition);
        view.append("\n");

        return view.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, name, updatedAllowed, insertAllowed, viewDefinition, columns, referencedTables);
    }
}


