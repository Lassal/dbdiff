package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.fs.FSManager;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.ParallelSerializer;
import br.lassal.dbvcs.tatubola.report.DBModelSerializationReport;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class ParallelDBVersionCommand implements Thread.UncaughtExceptionHandler{

    private static Logger logger = LoggerFactory.getLogger(ParallelDBVersionCommand.class);

    public static final int DEFAULT_PARALLELISM = 4;

    private List<String> schemas;
    private String tmpPath;
    private String rootPathLocalVCRepository;
    private List<DBModelSerializerBuilder> environments;
    private ForkJoinPool threadPool;
    private VersionControlSystem vcsController;
    private FSManager fsManager;


    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath
            , VersionControlSystem vcsController, FSManager fsManager, int parallelism) {
        this.schemas = schemas;
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.tmpPath = tmpPath;
        this.vcsController = vcsController;
        this.environments = new ArrayList<>();
        this.threadPool = this.createForkJoinPool(parallelism);
        this.fsManager = fsManager;

        if (this.vcsController != null) {
            this.vcsController.setWorkspacePath(new File(rootPathLocalVCRepository));
        }
    }

    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath
            , VersionControlSystem vcsController, FSManager fsManager) {
        this(schemas, rootPathLocalVCRepository, tmpPath, vcsController, fsManager, ParallelDBVersionCommand.DEFAULT_PARALLELISM);
    }

    public ParallelDBVersionCommand addDBEnvironment(DBModelSerializerBuilder serializerBuilder) {
        this.environments.add(serializerBuilder);

        return this;
    }

    private ForkJoinPool createForkJoinPool(int parallelism){
        return new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory, this, false);
    }

    public void takeDatabaseSchemaSnapshotVersion() throws Exception {

        EnvironmentInfo[] dbEnvs = new EnvironmentInfo[this.environments.size()];
        int envIndex = 0;

        this.vcsController.setupRepositoryInitialState();

        boolean listAllSchemas = this.schemas == null || this.schemas.isEmpty();

        if (logger.isDebugEnabled()) {
            logger.debug("(Step 1) Serialize each environment. Environment count = " + this.environments.size());
        }

        for (DBModelSerializerBuilder envBuilder : this.environments) {
            envBuilder.setOutputPath(this.tmpPath, true);

            if (logger.isDebugEnabled()) {
                logger.debug("(Step 2|" + envBuilder.getEnvironmentName() + ") Create serializers by environment");
            }

            List<DBModelSerializer> serializers = new ArrayList<>();
            if (listAllSchemas) {
                serializers = envBuilder.getDBModelSerializers();
            } else {
                for (String schema : this.schemas) {
                    List<DBModelSerializer> schemaSer = envBuilder
                            .getDBModelSerializers(schema);

                    serializers.addAll(schemaSer);
                }
            }


            EnvironmentInfo envInfo = new EnvironmentInfo(envBuilder, serializers.size());
            dbEnvs[envIndex] = envInfo;

            serializers.stream()
                    .forEach(s -> {
                        s.setMetricsListener(envInfo.getSerializerCounter());
                        threadPool.execute(
                           envInfo.addParallelSerializer(new ParallelSerializer(s, envInfo.getTaskSerializerLatch()))
                        );
                    });

            if (logger.isDebugEnabled()) {
                logger.debug("(Step 3|" + envBuilder.getEnvironmentName() + ") Start serialization in parallel thread");
            }

            envIndex++;
        }


        for (int i = 0; i < dbEnvs.length; i++) {

            this.waitEndSerializationAndCheckEnvBranch(dbEnvs[i]);

            this.moveDBSerializedObjectsToLocalRepository(dbEnvs[i]);

            this.commitEnvironmentSerialization(dbEnvs[i]);
        }

        logger.debug("(Step 7) About to sync changes to Server");
        this.vcsController.syncChangesToServer();

    }

    /**
     * Waits for all serialization tasks for this environment finishes and then checkout the corresponding branch
     * in the local repository
     *
     * @param envInfo All the information of the current branch
     * @throws InterruptedException
     * @throws VersionControlSystemException
     */
    private void waitEndSerializationAndCheckEnvBranch(EnvironmentInfo envInfo) throws Exception {
        envInfo.getTaskSerializerLatch().await();

        try {
            envInfo.checkFailedSerializers();
        } catch (Throwable sourceException) {
            throw new Exception("Detected error during object serialization in parallel. Check inner exception.",sourceException);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Branch : " + envInfo.getEnvName() + " | Remaining tasks: " + envInfo.getTaskSerializerLatch().getCount());
            logger.debug("(Step 4|" + envInfo.getEnvName() + ") About to check-out branch : " + envInfo.getEnvName());

        }

        this.vcsController.checkout(envInfo.getEnvName());

    }

    /**
     * Move all serialized objects of this environment generated in the temp folder and move it to
     * the local repository to detect the object changes.
     * Inside the temp folder there are a different folder for each environment
     *
     * @param envInfo
     * @throws IOException
     */
    private void moveDBSerializedObjectsToLocalRepository(EnvironmentInfo envInfo) throws IOException {
        File localRepoFolder = new File(this.rootPathLocalVCRepository);
        File dbEnvFiles = new File(envInfo.getOutputPath());

        if (logger.isDebugEnabled()) {
            logger.debug("(Step 5|" + envInfo.getEnvName() + ") About to copy files from " + dbEnvFiles + " to " + localRepoFolder);
        }

        this.fsManager.copyFullFolderStructure(dbEnvFiles.toPath(), localRepoFolder.toPath());
    }

    /**
     * Generate a report with all the serialization changes and commit all the local changes in the repository
     *
     *
     *
     * @param envInfo
     * @throws VersionControlSystemException
     */
    private void commitEnvironmentSerialization(EnvironmentInfo envInfo) throws VersionControlSystemException {
        if (logger.isDebugEnabled()) {
            logger.debug("(Step 6|" + envInfo.getEnvName() + ") About to commit changes in branch: " + envInfo.getEnvName());
        }

        DBModelSerializationReport report = new DBModelSerializationReport(envInfo.getEnvName(), envInfo.getDBJdbcUrl());

        report.writeSchemaSerializationMetrics(envInfo.getSerializerCounter().getMetrics());

        logger.info(report.print());

        this.vcsController.commitAllChanges(report.print());

    }

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        logger.error("Error in thread " + thread, throwable);
    }
}
