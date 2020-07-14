package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.text.SqlNormalizer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true, value = {"tableID"})
public class Table implements DatabaseModelEntity {

    private String name;
    private String schema;
    private List<TableColumn> columns;
    private List<TableConstraint> constraints;

    public Table() {
    }

    public Table(String schema, String name) {
        this(schema, name, true);
    }

    public Table(String schema, String name, boolean initializeInnerCollections) {
        this.setSchema(schema);
        this.name = name;
        if (initializeInnerCollections) {
            this.columns = new ArrayList<>();
            this.constraints = new ArrayList<>();
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    @Override
    public void tidyUpProperties(SqlNormalizer normalizer) {
        throw new UnsupportedOperationException();
    }


    public void addColumn(TableColumn column) {
        this.columns.add(column);
    }

    public List<TableColumn> getColumns() {
        return this.columns;
    }

    public void addConstraint(TableConstraint constraint) {
        this.constraints.add(constraint);
    }

    public List<TableConstraint> getConstraints() {
        return this.constraints;
    }

    public String getTableID() {
        return this.schema + "." + this.name;
    }

    public void onAfterLoad() {
        if (this.constraints != null) {
            this.constraints.sort(Comparator.comparing(c -> c.getType().getOrder() + "." + c.getName()));
        }
    }

    @Override
    public String toString() {
        StringBuilder tableStr = new StringBuilder();

        tableStr.append(String.format("Schema: %s >> Table: %s%n", this.schema, this.name));
        for (TableColumn column : this.columns) {
            tableStr.append(String.format("    %s%n", column));
        }

        tableStr.append("----- CONSTRAINTS -----\n");
        for (TableConstraint constraint : this.constraints) {
            tableStr.append(String.format("    %s%n", constraint));
        }


        return tableStr.toString();
    }


    @Override
    public boolean equals(Object other) {
        boolean isEqual = false;
        if (other instanceof Table) {
            Table otherT = (Table) other;
            isEqual = true;

            isEqual &= this.getSchema().equals(otherT.getSchema());
            isEqual &= this.getName().equals(otherT.getName());
            isEqual &= this.getColumns().size() == otherT.getColumns().size();
            isEqual &= this.getConstraints().size() == otherT.getConstraints().size();

            if (!isEqual) {
                return false;
            }

            Map<String, TableColumn> thisColumns = this.columns.stream()
                    .collect(Collectors.toMap(TableColumn::getName, Function.identity()));

            for (TableColumn c : otherT.getColumns()) {
                isEqual &= thisColumns.containsKey(c.getName());


                if (isEqual) {
                    isEqual &= thisColumns.get(c.getName()).equals(c);
                }

            }

            Map<String, TableConstraint> thisConstraints = this.constraints.stream()
                    .collect(Collectors.toMap(c -> this.schema + "." + c.getName(), Function.identity()));

            for (TableConstraint c : otherT.getConstraints()) {
                // retomar daqui
                String constraintID = this.schema + "." + c.getName();

                isEqual &= thisConstraints.containsKey(constraintID);

                if (isEqual) {
                    isEqual &= thisConstraints.get(constraintID).equals(c);
                }
            }
        }


        return isEqual;
    }


    @Override
    public int hashCode() {
        return Objects.hash(schema, name, columns, constraints);
    }
}
