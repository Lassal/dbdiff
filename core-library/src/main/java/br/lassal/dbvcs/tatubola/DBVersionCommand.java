package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DBVersionCommand {

    private List<String> schemas;
    private String rootPathLocalVCRepository;
    private VersionControlSystem vcsController;
    private List<DBModelSerializerBuilder> environments;


    public DBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, VersionControlSystem vcsController) {
        this.schemas = schemas;
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.environments = new ArrayList<>();
        this.vcsController = vcsController;

        this.vcsController.setWorkspacePath(new File(rootPathLocalVCRepository));
    }

    public DBVersionCommand addDBEnvironment(DBModelSerializerBuilder serializerBuilder) {
        this.environments.add(serializerBuilder);

        return this;
    }

    public void takeDatabaseSchemaSnapshotVersion() throws Exception {

        this.vcsController.setupRepositoryInitialState();

        boolean listAllSchemas = this.schemas == null || this.schemas.isEmpty();

        for (DBModelSerializerBuilder envBuilder : this.environments) {
            envBuilder.setOutputPath(this.rootPathLocalVCRepository, false);

            this.vcsController.checkout(envBuilder.getEnvironmentName());

            List<DBModelSerializer> serializers = new ArrayList<>();
            if (listAllSchemas) {
                serializers = envBuilder.getDBModelSerializers();
            } else {
                for (String schema : this.schemas) {
                    serializers.addAll(envBuilder.getDBModelSerializers(schema));
                }
            }

            for (DBModelSerializer serializer : serializers) {
                serializer.serialize();
            }

            this.vcsController.commitAllChanges("-- ?? add info in the message about the number of objects created ---");
        }

        this.vcsController.syncChangesToServer();
    }

}