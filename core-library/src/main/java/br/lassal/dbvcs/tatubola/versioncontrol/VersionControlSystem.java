package br.lassal.dbvcs.tatubola.versioncontrol;

public interface VersionControlSystem {

    void setupRepositoryInitialState();

    void checkout(String branch);

    void commitAllChanges(String changeMessage);

    void syncChangesToServer();
}
