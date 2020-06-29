package br.lassal.dbvcs.tatubola.integration;

public class IntegrationTestInfo {

    public static final String REMOTE_REPO = "https://github.com/Lassal/dbdiff-integration-tests-vcs.git";
    public static final String REPO_USER_ENV = "GITREPO_USER";
    public static final String REPO_PASSWORD_ENV = "GITREPO_PWD";
    public static final String REPO_BASE_BRANCH = "master";

    // Oracle integration test info
    public static final String ORACLE_JDBC_URL = "jdbc:oracle:thin:@//192.168.15.8:1521/orcl";
    public static final String ORACLE_USER_ENV = "ORACLE_USER";
    public static final String ORACLE_PASSWORD_ENV = "ORACLE_PWD";

    public static String getVCSRepositoryUsername(){

        return System.getenv(IntegrationTestInfo.REPO_USER_ENV);
    }

    public static String getVCSRepositoryPassword(){

        return System.getenv(IntegrationTestInfo.REPO_PASSWORD_ENV);
    }

    public static String getOracleUsername(){
        return System.getenv(IntegrationTestInfo.ORACLE_USER_ENV);
    }

    public static String getOraclePassword(){
        return System.getenv(IntegrationTestInfo.ORACLE_PASSWORD_ENV);
    }
}
