package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.ParallelSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DBVersionCommand {

    private List<String> schemas;
    private String rootPathLocalVCRepository;
    private List<DBModelSerializerBuilder> environments;


    public DBVersionCommand(List<String> schemas, String rootPathLocalVCRepository) {
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.environments = new ArrayList<>();
    }

    public void takeDatabaseSchemaSnapshotVersion() throws Exception {

        //TODO: setup local vcs repository

        boolean listAllSchemas = this.schemas == null || this.schemas.size() < 1;

        for (DBModelSerializerBuilder envBuilder : this.environments) {
            //TODO: create a serializer for each environment in  the repository output
            envBuilder.setOutputPath(this.rootPathLocalVCRepository, false);

            //TODO: VCS checkout repository for the env branch

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

            //TODO: VCS commit changes
        }

        //TODO: VCS push changes to server
    }

}