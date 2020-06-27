package br.lassal.dbvcs.tatubola.integration;


import br.lassal.dbvcs.tatubola.DBVersionCommand;
import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;
import br.lassal.dbvcs.tatubola.versioncontrol.GitController;
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

    private String getAbsolutePathRepository(String localRepositoryName){
        File repoPath = new File("test-repo/" + localRepositoryName);

        return repoPath.getAbsolutePath();
    }

    @Test
    public void versionSingleOracleDBSerial() throws Exception {
        List<String> schemas = this.getOracleTargetSchemas();
        GitController vcsController = new GitController();
        String repoOutputPath = this.getAbsolutePathRepository("singleDBOracle");
        String oraJdbcUrl = "jdbc:oracle:thin:@//192.168.15.8:1521/orcl";
        String dbUser = "app_data";
        String dbPwd = "oracle";

        logger.info("Output path: " + repoOutputPath);
        DBModelSerializerBuilder oraDB = new DBModelSerializerBuilder("DEV", oraJdbcUrl, dbUser, dbPwd);

        DBVersionCommand cmd = new DBVersionCommand(schemas, repoOutputPath, vcsController)
                .addDBEnvironment(oraDB);

        cmd.takeDatabaseSchemaSnapshotVersion();

    }

    private List<String> getOracleTargetSchemas(){
        List<String> schemas = new ArrayList<>();

        schemas.add("APEX_050000");
        schemas.add("APP_DATA");
        schemas.add("HR");
        schemas.add("XDB");

        return schemas;
    }
}
