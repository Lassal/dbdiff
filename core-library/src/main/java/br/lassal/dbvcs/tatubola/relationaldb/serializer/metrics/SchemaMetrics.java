package br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;

public class SchemaMetrics {

    private String schema;
    private int tables;
    private int views;
    private int indexes;
    private int triggers;
    private int routines;

    public SchemaMetrics(String schema){
        this.schema = schema;
        this.routines = 0;
        this.indexes = 0;
        this.views = 0;
        this.triggers = 0;
        this.tables = 0;
    }

    public String getSchema(){
        return this.schema;
    }

    public int increment(Class<? extends DatabaseModelEntity> type, int increment ){

        switch (type.getSimpleName()){
            case "Table": return this.tables += increment;
            case "Trigger" : return this.triggers += increment;
            case "View" : return this.views += increment;
            case "Index" : return this.indexes += increment;
            case "Routine" : return this.routines += increment;
            default: return 0;
        }
    }

    @Override
    public String toString() {

        return String.format("Schema: [%20s] %n  :: Tables: %05d | Views: %05d | Indexes: %05d | Triggers: %05d | Routines: %05d"
        , this.schema, this.tables, this.views, this.indexes, this.triggers, this.routines);

    }
}
