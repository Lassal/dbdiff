package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.builder.MockDatabaseSerializerFactory;
import br.lassal.dbvcs.tatubola.builder.ParallelDBVersionCommandBuilder;
import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.*;
import br.lassal.dbvcs.tatubola.relationaldb.repository.InMemoryTestRepository;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import br.lassal.dbvcs.tatubola.text.SqlNormalizer;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

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
        SqlNormalizer sqlNormalizer = SqlNormalizer.getInstance(repository);

        MockDatabaseSerializerFactory factory = new MockDatabaseSerializerFactory();
        factory.setRepository(repository);
        factory.setVcs(this.vcs);

        JacksonYamlSerializer textSerializer = new JacksonYamlSerializer();

        ParallelDBVersionCommand cmd =
                new ParallelDBVersionCommandBuilder(factory)
                        .setVCSRemoteInfo(remoteRepoUrl, baseBranch, username, password)
                        .setWorkspaceInfo(rootPath, tmpPath)
                        .setDBSchemasToBeSerialized(schemas)
                        .addDBEnvironment("DEV", "jdbc:dummy:dev", username, password)
                        .addDBEnvironment("QA", "jdbc:dummy:qa", username, password)
                        .build();

        cmd.takeDatabaseSchemaSnapshotVersion();

        verify(this.vcs, atMostOnce()).setupRepositoryInitialState();
        verify(this.vcs, atMostOnce()).checkout("DEV");
        verify(this.vcs, atMostOnce()).checkout("QA");
        verify(this.vcs, times(2)).commitAllChanges(anyString());
        verify(this.vcs, atMostOnce()).syncChangesToServer();

        InMemoryTestDBModelFS outputFS = factory.getDBModelFS(tmpPath + "/DEV");

        // Test tables serialized and deserialized
        List<Table> bTables = repository.getInnerTables(schemas[1]);

        for (Table sourceTable : bTables) {
            InMemoryTestDBModelFS.SerializationInfo serializedTableInfo = outputFS.getSerializationInfo(sourceTable);
            Table deserializedTable = textSerializer.fromYAMLtoPOJO(serializedTableInfo.getYamlText(), Table.class);

            assertEquals(sourceTable, deserializedTable);
        }

        // Test views serialized and deserialized
        List<View> aViews = repository.getInnerViews(schemas[0]);

        for (View sourceView : aViews) {
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceView);
            View deserializedView = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), View.class);

            sourceView.tidyUpProperties(sqlNormalizer);
            assertEquals(sourceView, deserializedView);
        }

        // Test indexes serialized and deserialized
        List<Index> bIndexes = repository.getInnerIndexes(schemas[1]);

        for (Index sourceIndex : bIndexes) {
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceIndex);
            Index deserializedIndex = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), Index.class);

            assertEquals(sourceIndex, deserializedIndex);
        }

        // Test triggers serialized and deserialized
        List<Trigger> cTriggers = repository.getInnerTriggers(schemas[2]);

        for (Trigger sourceTrigger : cTriggers) {
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceTrigger);
            Trigger deserializedTrigger = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), Trigger.class);

            sourceTrigger.tidyUpProperties(sqlNormalizer);
            assertEquals(sourceTrigger, deserializedTrigger);
        }

        // Test routines serialized and deserialized
        List<Routine> aRoutines = repository.getInnerRoutines(schemas[0]);

        for (Routine sourceRoutine : aRoutines) {
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceRoutine);
            Routine deserializedRoutine = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), Routine.class);

            sourceRoutine.tidyUpProperties(sqlNormalizer);
            assertEquals(sourceRoutine, deserializedRoutine);
        }

    }

    @Test
    public void testParallelVsSerialDatabaseSchemaSnapshotVersionEquality() throws Exception {

        String remoteRepoUrl = "remoteVCSUrl";
        String baseBranch = "MAIN";
        String username = "user";
        String password = "pwd";
        String rootPath = "parallelDBVersionCmdRoot/output";
        String tmpPath = "parallelDBVersionCmdRoot/tmp";
        String[] schemas = {"AAA", "BBB", "CCC"};

        InMemoryTestRepository repository = new InMemoryTestRepository();
        repository.fillRepositoryWithSampleDBObjects(schemas);
        SqlNormalizer sqlNormalizer = SqlNormalizer.getInstance(repository);

        MockDatabaseSerializerFactory factoryPar = new MockDatabaseSerializerFactory();
        factoryPar.setRepository(repository);
        factoryPar.setVcs(mock(VersionControlSystem.class));

        JacksonYamlSerializer textSerializer = new JacksonYamlSerializer();

        ParallelDBVersionCommand cmdPar =
                new ParallelDBVersionCommandBuilder(factoryPar)
                        .setVCSRemoteInfo(remoteRepoUrl, baseBranch, username, password)
                        .setWorkspaceInfo(rootPath, tmpPath)
                        .setDBSchemasToBeSerialized(schemas)
                        .addDBEnvironment("DEV", "jdbc:dummy:dev", username, password)
                        .addDBEnvironment("QA", "jdbc:dummy:qa", username, password)
                        .build();

        cmdPar.takeDatabaseSchemaSnapshotVersion();
        InMemoryTestDBModelFS outputFSPar = factoryPar.getDBModelFS(tmpPath + "/DEV");

        //create same serializer but now serial
        MockDatabaseSerializerFactory factorySer = new MockDatabaseSerializerFactory();
        factorySer.setRepository(repository);
        factorySer.setVcs(mock(VersionControlSystem.class));

        DBVersionCommand cmdSer = new DBVersionCommand(Arrays.asList(schemas), rootPath, this.vcs );
        cmdSer.addDBEnvironment(new DBModelSerializerBuilder(factorySer, "DEV", "jdbc:dummy:dev", username, password));
        cmdSer.addDBEnvironment(new DBModelSerializerBuilder(factorySer, "QA", "jdbc:dummy:qa", username, password));

        cmdSer.takeDatabaseSchemaSnapshotVersion();
        InMemoryTestDBModelFS outputFSSer = factorySer.getDBModelFS(rootPath);

        for(String schema : schemas){
            for(Table sourceTable : repository.getInnerTables(schema)){
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoParallel = outputFSPar.getSerializationInfo(sourceTable);
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoSerial = outputFSSer.getSerializationInfo(sourceTable);

                assertEquals(serializedTableInfoParallel.getYamlText(), serializedTableInfoSerial.getYamlText());
                assertEquals(serializedTableInfoParallel.getDBObject(), serializedTableInfoSerial.getDBObject());
            }

            for(View sourceView : repository.getInnerViews(schema)){
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoParallel = outputFSPar.getSerializationInfo(sourceView);
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoSerial = outputFSSer.getSerializationInfo(sourceView);

                assertEquals(serializedTableInfoParallel.getYamlText(), serializedTableInfoSerial.getYamlText());
                assertEquals(serializedTableInfoParallel.getDBObject(), serializedTableInfoSerial.getDBObject());
            }

            for(Index sourceIndex : repository.getInnerIndexes(schema)){
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoParallel = outputFSPar.getSerializationInfo(sourceIndex);
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoSerial = outputFSSer.getSerializationInfo(sourceIndex);

                assertEquals(serializedTableInfoParallel.getYamlText(), serializedTableInfoSerial.getYamlText());
                assertEquals(serializedTableInfoParallel.getDBObject(), serializedTableInfoSerial.getDBObject());
            }

            for(Trigger sourceTrigger : repository.getInnerTriggers(schema)){
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoParallel = outputFSPar.getSerializationInfo(sourceTrigger);
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoSerial = outputFSSer.getSerializationInfo(sourceTrigger);

                assertEquals(serializedTableInfoParallel.getYamlText(), serializedTableInfoSerial.getYamlText());
                assertEquals(serializedTableInfoParallel.getDBObject(), serializedTableInfoSerial.getDBObject());
            }

            for(Routine sourceRoutine : repository.getInnerRoutines(schema)){
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoParallel = outputFSPar.getSerializationInfo(sourceRoutine);
                InMemoryTestDBModelFS.SerializationInfo serializedTableInfoSerial = outputFSSer.getSerializationInfo(sourceRoutine);

                assertEquals(serializedTableInfoParallel.getYamlText(), serializedTableInfoSerial.getYamlText());
                assertEquals(serializedTableInfoParallel.getDBObject(), serializedTableInfoSerial.getDBObject());
            }

        }
    }
}