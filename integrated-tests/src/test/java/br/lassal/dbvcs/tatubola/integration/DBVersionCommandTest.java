package br.lassal.dbvcs.tatubola.integration;


import br.lassal.dbvcs.tatubola.DBVersionCommand;
import br.lassal.dbvcs.tatubola.ParallelDBVersionCommand;
import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.builder.DatabaseSerializerFactory;
import br.lassal.dbvcs.tatubola.builder.ParallelDBVersionCommandBuilder;
import br.lassal.dbvcs.tatubola.builder.RelationalDBVersionFactory;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DBVersionCommandTest {

    private static Logger logger = LoggerFactory.getLogger(DBVersionCommandTest.class);

    public DBVersionCommandTest(){

        this.initializeOutputFolder("singleDBOracle");
    }

    private void initializeOutputFolder(String folder){

        File outputPath = new File("test-repo/" + folder);

        try{

            if(!outputPath.exists()){
                outputPath.mkdirs();
            }

        }catch (Exception ex){
            logger.error("Could not create output folder to export local repository : " + outputPath.getAbsolutePath(), ex);
        }

    }

    private File getAbsolutePathRepository(String localRepositoryName){
        return new File("test-repo/" + localRepositoryName);
    }

    private List<String> getOracleTargetSchemas(){
        List<String> schemas = new ArrayList<>();


        //schemas.add("SYS");
       // schemas.add("APEX_050000");
        schemas.add("APP_DATA");
        schemas.add("HR");
       // schemas.add("XDB");

        return schemas;
    }

    @Test
    public void versionSingleOracleDBSerial() throws Exception {
        List<String> schemas = this.getOracleTargetSchemas();

        String gitRemoteUrl = IntegrationTestInfo.REMOTE_REPO;
        String baseBranch = IntegrationTestInfo.REPO_BASE_BRANCH;
        String repo_username = IntegrationTestInfo.getVCSRepositoryUsername();
        String repo_pwd = IntegrationTestInfo.getVCSRepositoryPassword();

        DatabaseSerializerFactory factory = RelationalDBVersionFactory.getInstance();
        VersionControlSystem vcsController = factory
                .createVCSController(gitRemoteUrl, repo_username, repo_pwd, baseBranch);

        File repoOutputPath = this.getAbsolutePathRepository("singleDBOracle");
        String oraJdbcUrl = IntegrationTestInfo.ORACLE_JDBC_URL;
        String dbUser =  IntegrationTestInfo.getOracleUsername();
        String dbPwd = IntegrationTestInfo.getOraclePassword();

        logger.info("Output path: " + repoOutputPath);

        DBVersionCommand cmd = new DBVersionCommand(schemas, repoOutputPath.getAbsolutePath(), vcsController)
                .addDBEnvironment(new DBModelSerializerBuilder(factory,"Oracle-DEV", oraJdbcUrl, dbUser, dbPwd))
                .addDBEnvironment(new DBModelSerializerBuilder(factory, "Oracle-QA", oraJdbcUrl, dbUser, dbPwd))
                .addDBEnvironment(new DBModelSerializerBuilder(factory, "Oracle-PROD", oraJdbcUrl, dbUser, dbPwd));

        //clean up local files
        boolean removeLocalRepositoryStatus = repoOutputPath.delete();
        logger.info(String.format("Remove local repository [%s] - SUCCESS: %s", repoOutputPath.getAbsolutePath(), removeLocalRepositoryStatus));

        cmd.takeDatabaseSchemaSnapshotVersion();

    }

    @Test
    public void versionOracleDBParallel() throws Exception {

        //VCS info
        String gitRemoteUrl = IntegrationTestInfo.REMOTE_REPO;
        String baseBranch = IntegrationTestInfo.REPO_BASE_BRANCH;
        String repo_username = IntegrationTestInfo.getVCSRepositoryUsername();
        String repo_pwd = IntegrationTestInfo.getVCSRepositoryPassword();

        // VCS local
        File repoOutputPath = this.getAbsolutePathRepository("ParallelDBOracle");

        //Oracle DB info
        String oraJdbcUrl = IntegrationTestInfo.ORACLE_JDBC_URL;
        String dbUser =  IntegrationTestInfo.getOracleUsername();
        String dbPwd = IntegrationTestInfo.getOraclePassword();
        DatabaseSerializerFactory factory = RelationalDBVersionFactory.getInstance();

        ParallelDBVersionCommand cmd =
                new ParallelDBVersionCommandBuilder(factory,8)
                        .setVCSRemoteInfo(gitRemoteUrl, baseBranch, repo_username, repo_pwd)
                        .setWorkspaceInfo(repoOutputPath.getAbsolutePath(), "tmp/parallel")
                        .setDBSchemasToBeSerialized(this.getOracleTargetSchemas())
                        .addDBEnvironment("Oracle-DEV", oraJdbcUrl, dbUser, dbPwd)
                        .addDBEnvironment("Oracle-QA", oraJdbcUrl, dbUser, dbPwd)
                        .addDBEnvironment("Oracle-PROD", oraJdbcUrl, dbUser, dbPwd)
                        .build();

        logger.info("Output path: " + repoOutputPath);

        //clean up local files
        boolean removeLocalRepositoryStatus = repoOutputPath.delete();
        logger.info(String.format("Remove local repository [%s] - SUCCESS: %s", repoOutputPath.getAbsolutePath(), removeLocalRepositoryStatus));

        cmd.takeDatabaseSchemaSnapshotVersion();

    }
}
