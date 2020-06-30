package br.lassal.dbvcs.tatubola.integration;


import br.lassal.dbvcs.tatubola.DBVersionCommand;
import br.lassal.dbvcs.tatubola.ParallelDBVersionCommand;
import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.builder.RelationalDBVersionFactory;
import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;
import br.lassal.dbvcs.tatubola.versioncontrol.GitController;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
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

        schemas.add("APEX_050000");
        schemas.add("APP_DATA");
        schemas.add("HR");
        schemas.add("XDB");

        return schemas;
    }

    @Test
    public void versionSingleOracleDBSerial() throws Exception {
        List<String> schemas = this.getOracleTargetSchemas();

        String gitRemoteUrl = IntegrationTestInfo.REMOTE_REPO;
        String baseBranch = IntegrationTestInfo.REPO_BASE_BRANCH;
        String repo_username = IntegrationTestInfo.getVCSRepositoryUsername();
        String repo_pwd = IntegrationTestInfo.getVCSRepositoryPassword();

        VersionControlSystem vcsController = RelationalDBVersionFactory.getInstance()
                .createVCSController(gitRemoteUrl, repo_username, repo_pwd, baseBranch);

        File repoOutputPath = this.getAbsolutePathRepository("singleDBOracle");
        String oraJdbcUrl = IntegrationTestInfo.ORACLE_JDBC_URL;
        String dbUser =  IntegrationTestInfo.getOracleUsername();
        String dbPwd = IntegrationTestInfo.getOraclePassword();

        logger.info("Output path: " + repoOutputPath);

        DBVersionCommand cmd = new DBVersionCommand(schemas, repoOutputPath.getAbsolutePath(), vcsController)
                .addDBEnvironment(new DBModelSerializerBuilder("Oracle-DEV", oraJdbcUrl, dbUser, dbPwd))
                .addDBEnvironment(new DBModelSerializerBuilder("Oracle-QA", oraJdbcUrl, dbUser, dbPwd))
                .addDBEnvironment(new DBModelSerializerBuilder("Oracle-PROD", oraJdbcUrl, dbUser, dbPwd));

        //clean up local files
        boolean removeLocalRepositoryStatus = repoOutputPath.delete();
        logger.info(String.format("Remove local repository [%s] - SUCCESS: %s", repoOutputPath.getAbsolutePath(), removeLocalRepositoryStatus));

        cmd.takeDatabaseSchemaSnapshotVersion();

    }

    @Test
    public void versionSingleOracleDBParallel() throws Exception {
        List<String> schemas = null;//this.getOracleTargetSchemas();

        String gitRemoteUrl = IntegrationTestInfo.REMOTE_REPO;
        String baseBranch = IntegrationTestInfo.REPO_BASE_BRANCH;
        String repo_username = IntegrationTestInfo.getVCSRepositoryUsername();
        String repo_pwd = IntegrationTestInfo.getVCSRepositoryPassword();

        VersionControlSystem vcsController = RelationalDBVersionFactory.getInstance()
                .createVCSController(gitRemoteUrl, repo_username, repo_pwd, baseBranch);

        File repoOutputPath = this.getAbsolutePathRepository("ParallelDBOracle");
        String oraJdbcUrl = IntegrationTestInfo.ORACLE_JDBC_URL;
        String dbUser =  IntegrationTestInfo.getOracleUsername();
        String dbPwd = IntegrationTestInfo.getOraclePassword();

        logger.info("Output path: " + repoOutputPath);

        ParallelDBVersionCommand cmd = new ParallelDBVersionCommand(schemas, repoOutputPath.getAbsolutePath()
                , "tmp/parallel", vcsController)
                .addDBEnvironment(new DBModelSerializerBuilder("Oracle-DEV", oraJdbcUrl, dbUser, dbPwd))
                .addDBEnvironment(new DBModelSerializerBuilder("Oracle-QA", oraJdbcUrl, dbUser, dbPwd))
                .addDBEnvironment(new DBModelSerializerBuilder("Oracle-PROD", oraJdbcUrl, dbUser, dbPwd));

        //clean up local files
        boolean removeLocalRepositoryStatus = repoOutputPath.delete();
        logger.info(String.format("Remove local repository [%s] - SUCCESS: %s", repoOutputPath.getAbsolutePath(), removeLocalRepositoryStatus));

        cmd.takeDatabaseSchemaSnapshotVersion();

    }

}
