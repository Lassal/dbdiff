package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.ParallelSerializer;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
    private VersionControlSystem vcsController;


    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath
            , VersionControlSystem vcsController, int parallelism){
        this.schemas = schemas;
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.tmpPath = tmpPath;
        this.vcsController = vcsController;
        this.environments = new ArrayList<>();
        this.threadPool = new ForkJoinPool(parallelism);

    }

    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath
            , VersionControlSystem vcsController){
        this(schemas, rootPathLocalVCRepository, tmpPath, vcsController, ParallelDBVersionCommand.DEFAULT_PARALLELISM);
    }

    public ParallelDBVersionCommand addDBEnvironment(DBModelSerializerBuilder serializerBuilder){
        this.environments.add(serializerBuilder);

        return this;
    }

    public void takeDatabaseSchemaSnapshotVersion() throws Exception {

        String[] envBranches = new String[this.environments.size()];
        String[] sourceFolder = new String[this.environments.size()];
        RecursiveAction[] lastEnvActions = new RecursiveAction[this.environments.size()];
        int envIndex = 0;

        //TODO: setup local vcs repository
        this.vcsController.setupRepositoryInitialState();

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
            sourceFolder[envIndex] = envBuilder.getNormalizedEnvironmentName();
            envIndex++;

            // TODO: verify how to check when everything is processed
            serializers.stream().forEach(s -> threadPool.execute(s));
        }

        File repositoryFolder = new File(this.rootPathLocalVCRepository);

        for(int i=0; i < lastEnvActions.length; i++){
            lastEnvActions[i].join();

            //TODO: checkout branch
            this.vcsController.checkout(envBranches[i]);
            //TODO: move local files in TMP to repository
            File envFiles = new File(this.tmpPath, sourceFolder[i]);
            this.copyFullFolderStructure(envFiles.toPath(), repositoryFolder.toPath());
            //TODO: commit changes
            this.vcsController.commitAllChanges("Commited parallel environment");
        }

        //TODO: push changes to the server
        this.vcsController.syncChangesToServer();
    }

    private void copyFullFolderStructure(Path sourceDir, Path destinationDir) throws IOException {

        // Traverse the file tree and copy each file/directory.
        Files.walk(sourceDir)
                .forEach(sourcePath -> {
                    try {
                        Path targetPath = destinationDir.resolve(sourceDir.relativize(sourcePath));
                        System.out.printf("Copying %s to %s%n", sourcePath, targetPath);
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException ex) {
                        System.out.format("I/O error: %s%n", ex);
                    }
                });
    }
}
