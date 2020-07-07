package br.lassal.dbvcs.tatubola.versioncontrol;

import java.io.File;

public interface VersionControlSystem {

    void setWorkspacePath(File absolutePath);
    File getWorkspacePath();

    void setupRepositoryInitialState() throws Exception;

    void checkout(String branchName) throws Exception;

    void commitAllChanges(String changeMessage) throws Exception;

    void syncChangesToServer() throws Exception;
}
