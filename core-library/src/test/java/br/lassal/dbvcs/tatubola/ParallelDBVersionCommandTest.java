package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.ParallelDBVersionCommandBuilder;
import br.lassal.dbvcs.tatubola.builder.MockDatabaseSerializerFactory;
import br.lassal.dbvcs.tatubola.relationaldb.repository.InMemoryTestRepository;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ParallelDBVersionCommandTest {


    @Mock
    private VersionControlSystem vcs;

    @Test
    public void testTakeDatabaseSchemaSnapshotVersion() throws Exception {

        String remoteRepoUrl = "remoteVCSUrl";
        String baseBranch = "MAIN";
        String username = "user";
        String password = "pwd";
        String rootPath = "parallelDBVersionCmdRoot/output";
        String tmpPath = "parallelDBVersionCmdRoot/tmp";
        String[] schemas = {"AAA", "BBB", "CCC"};

        InMemoryTestRepository repository = new InMemoryTestRepository();
        repository.fillRepositoryWithSampleDBObjects(schemas);

        MockDatabaseSerializerFactory factory = new MockDatabaseSerializerFactory();
        factory.setRepository(repository);
        factory.setVcs(this.vcs);


        ParallelDBVersionCommand cmd =
                new ParallelDBVersionCommandBuilder(factory)
                .setVCSRemoteInfo(remoteRepoUrl, baseBranch, username, password)
                .setWorkspaceInfo(rootPath, tmpPath)
                .setDBSchemasToBeSerialized(schemas)
                .addDBEnvironment("DEV","jdbc:dummy:dev", username, password)
                .addDBEnvironment("QA","jdbc:dummy:qa", username, password)
                .build();

        cmd.takeDatabaseSchemaSnapshotVersion();
    }
}