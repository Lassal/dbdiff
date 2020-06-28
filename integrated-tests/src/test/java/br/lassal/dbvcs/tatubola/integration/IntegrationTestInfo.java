package br.lassal.dbvcs.tatubola.integration;

public class IntegrationTestInfo {

    public static final String REMOTE_REPO = "https://github.com/Lassal/dbdiff-integration-tests-vcs.git";
    public static final String REPO_USER_ENV = "GITREPO_USER";
    public static final String REPO_PASSWORD_ENV = "GITREPO_PWD";
    public static final String REPO_BASE_BRANCH = "master";

    public static String getVCSRepositoryUsername(){

        return System.getenv(IntegrationTestInfo.REPO_USER_ENV);
    }

    public static String getVCSRepositoryPassword(){

        return System.getenv(IntegrationTestInfo.REPO_PASSWORD_ENV);
    }


}
