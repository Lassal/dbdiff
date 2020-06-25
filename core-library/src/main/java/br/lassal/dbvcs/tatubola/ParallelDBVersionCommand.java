package br.lassal.dbvcs.tatubola;

import java.util.List;

public class ParallelDBVersionCommand {

    private String tmpPath;
    private String rootPathLocalVCRepository;
    private List<DBEnvironmentInfo> environments;


    public ParallelDBVersionCommand(List<DBEnvironmentInfo> envs, String rootPathLocalVCRepository, String tmpPath){
        this.environments = envs;
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.tmpPath = tmpPath;

        //TODO: initialize database serializers
    }

    public void takeDatabaseSchemaSnapshotVersion(){

        for (DBEnvironmentInfo env: this.environments ) {
           //TODO: create a serializer for each environment in  tmp path
        }

        //TODO: move each environment files and commit

        //TODO: push changes to the server
    }
}
