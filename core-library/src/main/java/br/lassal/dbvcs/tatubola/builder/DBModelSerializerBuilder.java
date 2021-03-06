package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DBModelSerializerBuilder {



    private DatabaseSerializerFactory factory;
    private String environmentName;
    private DBModelFS outputFS;
    private String outputPath;
    private final RelationalDBRepository repository;
    private String normalizedEnvironmentName = null;
    private String jdbcUrl = null;

    public DBModelSerializerBuilder(DatabaseSerializerFactory factory, String environmentName, String jdbcUrl, String username, String password) {

        this.factory = factory;
        this.environmentName = environmentName;
        this.repository = this.factory.createRDBRepository(jdbcUrl, username, password);
        this.jdbcUrl = jdbcUrl;

    }

    public String getEnvironmentName() {
        return this.environmentName;
    }

    private String getNormalizedEnvironmentName() {
        if (this.normalizedEnvironmentName == null && this.environmentName != null) {
            this.normalizedEnvironmentName = this.normalizeEnvNameAsPath(this.environmentName);
        }

        return this.normalizedEnvironmentName;
    }

    public String getJdbcUrl(){
        return this.jdbcUrl;
    }

    /**
     * Setup the output path where the database objects will be serialized as text files.
     * All files follow directory structure to represent the different object types, segregated
     * by schema.
     * It is also possible to segregate the output by environment using the @segregateByEnvironment property
     *
     * @param outputPath             output where all files will be generated
     * @param segregateByEnvironment TRUE adds a normalized environment folder in the output path
     *                               outputPath/(normalized environmentName)/(object types)
     *                               FALSE put all object types from the outputPath
     *                               outputPath/(object types)
     * @return builder
     */
    public DBModelSerializerBuilder setOutputPath(String outputPath, boolean segregateByEnvironment) {
        this.outputPath = segregateByEnvironment
                ? this.generateEnvOutputPath(outputPath).toString()
                : outputPath;

        this.outputFS = this.factory.createDBModelFS(this.outputPath);
        return this;
    }

    public String getOutputPath(){
        return this.outputPath;
    }

    private Path generateEnvOutputPath(String rootPath) {
        return Paths.get(rootPath, this.getNormalizedEnvironmentName());
    }

    /**
     * Normalize environment name removing special characters, replacing spaces by
     * underscores and capitalizing everything.
     *
     * @param environmentName
     * @return
     */
    private String normalizeEnvNameAsPath(String environmentName) {
        return environmentName.replaceAll("[^a-zA-Z\\d\\s:]*", "")
                .trim().replaceAll("\\s", "_")
                .toUpperCase();
    }

    public List<DBModelSerializer> getDBModelSerializers(String schema) {
        return this.factory
                .createDBObjectsSerializers(this.environmentName, schema, this.repository, this.outputFS);
    }

    public List<DBModelSerializer> getDBModelSerializers() {
        List<DBModelSerializer> allSerializers = new ArrayList<>();

        List<String> schemas = this.repository.listSchemas();

        if (schemas != null && !schemas.isEmpty()) {
            for (String schema : schemas) {
                List<DBModelSerializer> schemaSerializers = this.getDBModelSerializers(schema);
                allSerializers.addAll(schemaSerializers);
            }
        }

        return allSerializers;
    }
}
