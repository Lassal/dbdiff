package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.text.SqlNormalizer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonPropertyOrder({"schema","name","associateTableSchema", "associateTableName", "indexType", "unique", "columns"})
@JsonIgnoreProperties(ignoreUnknown = true, value = {})
public class Index implements DatabaseModelEntity {


    private String indexSchema;
    private String indexName;
    private String associateTableName;
    private String associateTableSchema;
    private String indexType;
    private boolean unique;
    List<IndexColumn> columns;

    public Index() {
        // for serialization purposes
        this.columns = new ArrayList<>();
    }

    public Index(String indexSchema, String indexName, String targetTableSchema, String targetTable, String indexType, boolean isUnique) {
        this.indexSchema = indexSchema;
        this.indexName = indexName;
        this.setAssociateTableSchema(targetTableSchema);
        this.setAssociateTableName(targetTable);
        this.setIndexType(indexType);
        this.setUnique(isUnique);
        this.columns = new ArrayList<>();
    }

    @Override
    public String getSchema() {
        return this.indexSchema;
    }

    public void setSchema(String schema) {
        this.indexSchema = schema;
    }

    @Override
    public String getName() {
        return this.indexName;
    }

    @Override
    public void tidyUpProperties(SqlNormalizer normalizer) {
        this.columns.sort(IndexColumn.DEFAULT_SORT_ORDER);
    }

    public void setName(String name) {
        this.indexName = name;
    }


    public String getAssociateTableName() {
        return associateTableName;
    }

    public void setAssociateTableName(String associateTableName) {
        this.associateTableName = associateTableName;
    }

    public String getAssociateTableSchema() {
        return associateTableSchema;
    }

    public void setAssociateTableSchema(String associateTableSchema) {
        this.associateTableSchema = associateTableSchema;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public void addColumn(IndexColumn column) {
        this.columns.add(column);
    }

    public List<IndexColumn> getColumns() {
        return this.columns;
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof Index) {
            Index other = (Index) obj;
            isEqual = true;

            isEqual &= this.indexSchema.equals(other.indexSchema);
            isEqual &= this.indexName.equals(other.indexName);
            isEqual &= this.associateTableSchema.equals(other.associateTableSchema);
            isEqual &= this.associateTableName.equals(other.associateTableName);
            isEqual &= this.indexType.equals(other.indexType);
            isEqual &= this.unique == other.unique;
            isEqual &= this.columns.size() == other.columns.size();

            if (isEqual) {

                for(int i=0; i < this.columns.size(); i++){
                    isEqual &= this.columns.get(i).equals(other.columns.get(i));
                }

            }

        }

        return isEqual;
    }

    @Override
    public String toString() {
        StringBuilder indexStr = new StringBuilder();

        indexStr.append(String.format("Schema: %s >> Table: %s >> Index %s%n", this.getSchema(), this.getAssociateTableName(), this.getName()));
        for (IndexColumn column : this.columns) {
            indexStr.append(String.format("    %s%n", column));
        }

        return indexStr.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexSchema, indexName, associateTableName, associateTableSchema, indexType, unique, columns);
    }
}
