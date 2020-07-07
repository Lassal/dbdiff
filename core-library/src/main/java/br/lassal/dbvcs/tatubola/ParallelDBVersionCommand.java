package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.ParallelSerializer;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ParallelDBVersionCommand {

    private static Logger logger = LoggerFactory.getLogger(ParallelDBVersionCommand.class);

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

        if(this.vcsController != null){
            this.vcsController.setWorkspacePath(new File(rootPathLocalVCRepository));
        }

    }

    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath
            , VersionControlSystem vcsController){
        this(schemas, rootPathLocalVCRepository, tmpPath, vcsController, ParallelDBVersionCommand.DEFAULT_PARALLELISM);
    }

    public ParallelDBVersionCommand addDBEnvironment(DBModelSerializerBuilder serializerBuilder){
        this.environments.add(serializerBuilder);

        return this;
    }

    /*
    public void takeDatabaseSchemaSnapshotVersion() throws Exception {

        String[] envBranches = new String[this.environments.size()];
        String[] sourceFolder = new String[this.environments.size()];
        RecursiveAction[] lastEnvActions = new RecursiveAction[this.environments.size()];
        int envIndex = 0;

        this.vcsController.setupRepositoryInitialState();

        boolean listAllSchemas = this.schemas == null || this.schemas.size() < 1;

        if(logger.isDebugEnabled()){
            logger.debug("(Step 1) Serialize each environment. Environment count = " + this.environments.size());
        }

        for (DBModelSerializerBuilder envBuilder: this.environments ) {
            envBuilder.setOutputPath(this.tmpPath, true);

            if(logger.isDebugEnabled()){
                logger.debug("(Step 2|" + envBuilder.getEnvironmentName() + ") Create serializers by environment");
            }

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

            if(logger.isDebugEnabled()){
                logger.debug("(Step 3|" + envBuilder.getEnvironmentName() + ") Start serialization in parallel thread");
            }

        }

        File repositoryFolder = new File(this.rootPathLocalVCRepository);

        for(int i=0; i < lastEnvActions.length; i++){
            //TODO: verify that all tasks have been completed; the last task may not be the last to execute
            lastEnvActions[i].join();

            if(logger.isDebugEnabled()){
                logger.debug("(Step 4|" + envBranches[i] + ") About to check-out branch : " + envBranches[i]);
            }
            this.vcsController.checkout(envBranches[i]);

            File envFiles = new File(this.tmpPath, sourceFolder[i]);
            if(logger.isDebugEnabled()){
                logger.debug("(Step 5|" + envBranches[i] + ") About to copy files from " + envFiles + " to " + repositoryFolder);
            }
            this.copyFullFolderStructure(envFiles.toPath(), repositoryFolder.toPath());

            if(logger.isDebugEnabled()){
                logger.debug("(Step 6|" + envBranches[i] + ") About to commit changes in branch: " + envBranches[i]);
            }

            this.vcsController.commitAllChanges("Commited parallel environment");
        }

        logger.debug("(Step 7) About to sync changes to Server");
        this.vcsController.syncChangesToServer();
    }
*/

    private void copyFullFolderStructure(Path sourceDir, Path destinationDir) throws IOException {

        // Traverse the file tree and copy each file/directory.
        Files.walk(sourceDir)
                .forEach(sourcePath -> {
                    try {
                        Path targetPath = destinationDir.resolve(sourceDir.relativize(sourcePath));

                        if(logger.isTraceEnabled()){
          //                  logger.trace(String.format("Copying %s to %s%n", sourcePath, targetPath));
                        }
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException ex) {
          //              logger.warn(String.format("I/O error: %s%n", ex));
                    }
                });
    }

    public void takeDatabaseSchemaSnapshotVersion() throws Exception {

        String[] envBranches = new String[this.environments.size()];
        String[] sourceFolder = new String[this.environments.size()];
        CountDownLatch[] allTasksEnv = new CountDownLatch[this.environments.size()];
        RecursiveAction[] lastEnvActions = new RecursiveAction[this.environments.size()];
        int envIndex = 0;

        this.vcsController.setupRepositoryInitialState();

        boolean listAllSchemas = this.schemas == null || this.schemas.size() < 1;

        if(logger.isDebugEnabled()){
            logger.debug("(Step 1) Serialize each environment. Environment count = " + this.environments.size());
        }

        for (DBModelSerializerBuilder envBuilder: this.environments ) {
            envBuilder.setOutputPath(this.tmpPath, true);

            if(logger.isDebugEnabled()){
                logger.debug("(Step 2|" + envBuilder.getEnvironmentName() + ") Create serializers by environment");
            }

            List<DBModelSerializer> serializers = new ArrayList<>();
            if(listAllSchemas){
                serializers = envBuilder.getDBModelSerializers();
            }
            else{
                for(String schema : this.schemas){
                    List<DBModelSerializer> schemaSer = envBuilder
                            .getDBModelSerializers(schema);

                    serializers.addAll(schemaSer);
                }
            }
            CountDownLatch envTotalTasks = new CountDownLatch(serializers.size());

            //TODO: temp change
            List<ParallelSerializer> parallelTasks = serializers.stream()
                    .map(s -> new ParallelSerializer(s, envTotalTasks))
                    .collect(Collectors.toList());
            lastEnvActions[envIndex] = parallelTasks.get(parallelTasks.size()-1);


            allTasksEnv[envIndex] = envTotalTasks;
            envBranches[envIndex] = envBuilder.getEnvironmentName();
            sourceFolder[envIndex] = envBuilder.getNormalizedEnvironmentName();
            envIndex++;

            // TODO: verify how to check when everything is processed
            //serializers.stream().forEach(s -> threadPool.execute(new ParallelSerializer(s, envTotalTasks)));
            parallelTasks.stream().forEach(t -> threadPool.execute(t));

            if(logger.isDebugEnabled()){
                logger.debug("(Step 3|" + envBuilder.getEnvironmentName() + ") Start serialization in parallel thread");
            }

        }

        File repositoryFolder = new File(this.rootPathLocalVCRepository);

        for(int i=0; i < envBranches.length; i++){
            //TODO: verify that all tasks have been completed; the last task may not be the last to execute
            allTasksEnv[i].await();
            //lastEnvActions[i].join();
            logger.debug("Branch : " +envBranches[i] + " | Remaining tasks: " + allTasksEnv[i].getCount());

            if(logger.isDebugEnabled()){
                logger.debug("(Step 4|" + envBranches[i] + ") About to check-out branch : " + envBranches[i]);
            }
            this.vcsController.checkout(envBranches[i]);

            File envFiles = new File(this.tmpPath, sourceFolder[i]);
            if(logger.isDebugEnabled()){
                logger.debug("(Step 5|" + envBranches[i] + ") About to copy files from " + envFiles + " to " + repositoryFolder);
            }
            this.copyFullFolderStructure(envFiles.toPath(), repositoryFolder.toPath());

            if(logger.isDebugEnabled()){
                logger.debug("(Step 6|" + envBranches[i] + ") About to commit changes in branch: " + envBranches[i]);
            }

            this.vcsController.commitAllChanges("Commited parallel environment");
        }

        logger.debug("(Step 7) About to sync changes to Server");
        this.vcsController.syncChangesToServer();

        threadPool.awaitQuiescence(150, TimeUnit.SECONDS);

        for(int i=0; i < allTasksEnv.length; i++){
            logger.debug("Last counter|| Branch : " +envBranches[i] + " | Remaining tasks: " + allTasksEnv[i].getCount());

        }
    }

}
