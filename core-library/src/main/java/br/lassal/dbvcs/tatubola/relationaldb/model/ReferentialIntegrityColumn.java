package br.lassal.dbvcs.tatubola.relationaldb.model;

public class ReferentialIntegrityColumn extends Column{

    private String referencedSchemaName;
    private String referencedTableName;
    private String referencedTableColumnName;


    public ReferentialIntegrityColumn(){}

    public ReferentialIntegrityColumn(String constraintColumnName, int ordinalPosition){
        super(constraintColumnName, ordinalPosition);
    }

    public String getReferencedSchemaName() {
        return referencedSchemaName;
    }

    public void setReferencedSchemaName(String referencedSchemaName) {
        this.referencedSchemaName = referencedSchemaName;
    }

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public void setReferencedTableName(String referencedTableName) {
        this.referencedTableName = referencedTableName;
    }

    public String getReferencedTableColumnName() {
        return referencedTableColumnName;
    }

    public void setReferencedTableColumnName(String referencedTableColumnName) {
        this.referencedTableColumnName = referencedTableColumnName;
    }

    public String toString(){
        return String.format("%s -> %s.%s.%s", this.getName(), this.referencedSchemaName, this.referencedTableName, this.referencedTableColumnName);
    }
}
