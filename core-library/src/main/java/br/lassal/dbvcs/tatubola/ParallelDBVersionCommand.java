package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.ParallelSerializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.stream.Collectors;

public class ParallelDBVersionCommand {

    public static final int DEFAULT_PARALLELISM = 4;

    private List<String> schemas;
    private String tmpPath;
    private String rootPathLocalVCRepository;
    private List<DBModelSerializerBuilder> environments;
    private ForkJoinPool threadPool;


    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath, int parallelism){
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.tmpPath = tmpPath;
        this.environments = new ArrayList<>();
        ForkJoinPool forkJoinPool = new ForkJoinPool(parallelism);


    }

    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath){
        this(schemas, rootPathLocalVCRepository, tmpPath, ParallelDBVersionCommand.DEFAULT_PARALLELISM);
    }

    public ParallelDBVersionCommand addDBEnvironment(DBModelSerializerBuilder serializerBuilder){
        this.environments.add(serializerBuilder);

        return this;
    }

    public void takeDatabaseSchemaSnapshotVersion(){

        String[] envBranches = new String[this.environments.size()];
        RecursiveAction[] lastEnvActions = new RecursiveAction[this.environments.size()];
        int envIndex = 0;

        //TODO: setup local vcs repository

        boolean listAllSchemas = this.schemas == null || this.schemas.size() < 1;

        for (DBModelSerializerBuilder envBuilder: this.environments ) {
           //TODO: create a serializer for each environment in  tmp path
            envBuilder.setOutputPath(this.tmpPath, true);

            List<ParallelSerializer> serializers = new ArrayList<>();
            if(listAllSchemas){
                serializers = envBuilder.getDBModelSerializers()
                        .stream().map(s -> new ParallelSerializer(s))
                        .collect(Collectors.toList());
            }
            else{
                for(String schema : this.schemas){
                    List<ParallelSerializer> schemaSer = envBuilder
                            .getDBModelSerializers(schema).stream()
                            .map(s -> new ParallelSerializer(s))
                            .collect(Collectors.toList());

                    serializers.addAll(schemaSer);
                }
            }

            lastEnvActions[envIndex] = serializers.get(serializers.size()-1);
            envBranches[envIndex] = envBuilder.getEnvironmentName();
            envIndex++;

            // TODO: verify how to check when everything is processed
            serializers.stream().forEach(s -> this.threadPool.execute(s));
        }

        for(int i=0; i < lastEnvActions.length; i++){
            lastEnvActions[i].join();

            //TODO: checkout branch
            //TODO: move local files in TMP to repository
            //TODO: commit changes
        }

        //TODO: push changes to the server
    }
}
