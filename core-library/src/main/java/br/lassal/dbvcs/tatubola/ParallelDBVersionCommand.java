package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
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
            , VersionControlSystem vcsController, int parallelism) {
        this.schemas = schemas;
        this.rootPathLocalVCRepository = rootPathLocalVCRepository;
        this.tmpPath = tmpPath;
        this.vcsController = vcsController;
        this.environments = new ArrayList<>();
        this.threadPool = new ForkJoinPool(parallelism);

        if (this.vcsController != null) {
            this.vcsController.setWorkspacePath(new File(rootPathLocalVCRepository));
        }
    }

    public ParallelDBVersionCommand(List<String> schemas, String rootPathLocalVCRepository, String tmpPath
            , VersionControlSystem vcsController) {
        this(schemas, rootPathLocalVCRepository, tmpPath, vcsController, ParallelDBVersionCommand.DEFAULT_PARALLELISM);
    }

    public ParallelDBVersionCommand addDBEnvironment(DBModelSerializerBuilder serializerBuilder) {
        this.environments.add(serializerBuilder);

        return this;
    }

    private void copyFullFolderStructure(Path sourceDir, Path destinationDir) throws IOException {

        // Traverse the file tree and copy each file/directory.
        Files.walk(sourceDir)
                .forEach(sourcePath -> {
                    try {
                        Path targetPath = destinationDir.resolve(sourceDir.relativize(sourcePath));

                        if (logger.isTraceEnabled()) {
                            //     logger.trace(String.format("Copying %s to %s", sourcePath, targetPath));
                        }
                        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException ex) {
                        // logger.warn(String.format("I/O error: %s%n", ex));
                    }
                });
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
                    .forEach(s -> threadPool.execute(
                            new ParallelSerializer(s.setMetricsListener(envInfo.getSerializerCounter()), envInfo.getTaskSerializerLatch())));

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

    private void waitEndSerializationAndCheckEnvBranch(EnvironmentInfo envInfo) throws InterruptedException, VersionControlSystemException {
        envInfo.getTaskSerializerLatch().await();

        if (logger.isDebugEnabled()) {
            logger.debug("Branch : " + envInfo.getEnvName() + " | Remaining tasks: " + envInfo.getTaskSerializerLatch().getCount());
            logger.debug("(Step 4|" + envInfo.getEnvName() + ") About to check-out branch : " + envInfo.getEnvName());

        }

        this.vcsController.checkout(envInfo.getEnvName());

    }

    private void moveDBSerializedObjectsToLocalRepository(EnvironmentInfo envInfo) throws IOException {
        File localRepoFolder = new File(this.rootPathLocalVCRepository);
        File dbEnvFiles = new File(this.tmpPath, envInfo.getSourceFolder());

        if (logger.isDebugEnabled()) {
            logger.debug("(Step 5|" + envInfo.getEnvName() + ") About to copy files from " + dbEnvFiles + " to " + localRepoFolder);
        }

        this.copyFullFolderStructure(dbEnvFiles.toPath(), localRepoFolder.toPath());

    }

    private void commitEnvironmentSerialization(EnvironmentInfo envInfo) throws VersionControlSystemException {
        if (logger.isDebugEnabled()) {
            logger.debug("(Step 6|" + envInfo.getEnvName() + ") About to commit changes in branch: " + envInfo.getEnvName());
        }

        DBModelSerializationReport report = new DBModelSerializationReport(envInfo.getEnvName(), envInfo.getDBJdbcUrl());

        report.writeSchemaSerializationMetrics(envInfo.getSerializerCounter().getMetrics());

        logger.info(report.print());

        this.vcsController.commitAllChanges(report.print());

    }

}
