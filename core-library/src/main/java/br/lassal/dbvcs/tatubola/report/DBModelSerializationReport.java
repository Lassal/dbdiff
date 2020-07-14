package br.lassal.dbvcs.tatubola.report;

import br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics.SchemaMetrics;

import java.time.Instant;
import java.util.List;

public class DBModelSerializationReport {

    private StringBuilder report;

    public DBModelSerializationReport(String environmentName, String jdbcUrl){
        this.report = new StringBuilder("Tatubola database environment version snapshot\n");
        this.report.append("  Taken at: " + Instant.now() + "\n\n");

        this.writeEnvironmentInfo(environmentName, jdbcUrl);
    }

    public DBModelSerializationReport writeEnvironmentInfo(String environmentName, String jdbcUrl){

        this.report.append("  ENVIRONMENT NAME: ");
        this.report.append(environmentName);
        this.report.append("\n  JDBC URL: ");
        this.report.append(jdbcUrl);
        this.report.append("\n");

        return this;
    }

    public DBModelSerializationReport writeSchemaSerializationMetrics(List<SchemaMetrics> schemaMetrics){
        this.report.append("\n  OBJECTS SERIALIZED BY SCHEMA:\n\n");

        for(SchemaMetrics schema : schemaMetrics){
            this.report.append(schema + "\n");
        }

        return this;
    }

    public String print(){
        return this.report.toString();
    }
}
