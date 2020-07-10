package br.lassal.dbvcs.tatubola.versioncontrol;

import java.io.File;

public interface VersionControlSystem {

    void setWorkspacePath(File absolutePath);

    File getWorkspacePath();

    void setupRepositoryInitialState() throws VersionControlSystemException;

    void checkout(String branchName) throws VersionControlSystemException;

    void commitAllChanges(String changeMessage) throws VersionControlSystemException;

    void syncChangesToServer() throws VersionControlSystemException;
}
