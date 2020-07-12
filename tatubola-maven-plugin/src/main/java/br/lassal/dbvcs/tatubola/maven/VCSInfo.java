package br.lassal.dbvcs.tatubola.maven;

public class VCSInfo {

    private String remoteRepositoryUrl;
    private String sourceBranch;
    private PlainCredentials credentials;


    public String getRemoteRepositoryUrl() {
        return remoteRepositoryUrl;
    }

    public void setRemoteRepositoryUrl(String remoteRepositoryUrl) {
        this.remoteRepositoryUrl = remoteRepositoryUrl;
    }

    public String getSourceBranch() {
        return sourceBranch;
    }

    public void setSourceBranch(String sourceBranch) {
        this.sourceBranch = sourceBranch;
    }

    public PlainCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(PlainCredentials credentials) {
        this.credentials = credentials;
    }
}
