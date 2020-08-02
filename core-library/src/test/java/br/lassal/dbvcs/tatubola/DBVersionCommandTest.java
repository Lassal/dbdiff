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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DBVersionCommandTest {

    @Mock
    private VersionControlSystem vcs;


    @Test
    public void takeDatabaseSchemaSnapshotVersion() throws Exception{
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

        DBVersionCommand cmd = new DBVersionCommand(Arrays.asList(schemas), rootPath, this.vcs );
        cmd.addDBEnvironment(new DBModelSerializerBuilder(factory, "DEV", "jdbc:dummy:dev", username, password));
        cmd.addDBEnvironment(new DBModelSerializerBuilder(factory, "QA", "jdbc:dummy:qa", username, password));

        cmd.takeDatabaseSchemaSnapshotVersion();

        verify(this.vcs, atMostOnce()).setupRepositoryInitialState();
        verify(this.vcs, atMostOnce()).checkout("DEV");
        verify(this.vcs, atMostOnce()).checkout("QA");
        verify(this.vcs, times(2)).commitAllChanges(anyString());
        verify(this.vcs, atMostOnce()).syncChangesToServer();

        InMemoryTestDBModelFS outputFS = factory.getDBModelFS(rootPath);

        // Test tables serialized and deserialized
        List<Table> bTables = repository.getInnerTables(schemas[1]);

        for(Table sourceTable: bTables){
            InMemoryTestDBModelFS.SerializationInfo serializedTableInfo = outputFS.getSerializationInfo(sourceTable);
            Table deserializedTable = textSerializer.fromYAMLtoPOJO(serializedTableInfo.getYamlText(), Table.class);

            assertEquals(sourceTable, deserializedTable);
        }

        // Test views serialized and deserialized
        List<View> aViews = repository.getInnerViews(schemas[0]);

        for(View sourceView: aViews){
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceView);
            View deserializedView = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), View.class);

            sourceView.tidyUpProperties(sqlNormalizer);
            assertEquals(sourceView, deserializedView);
        }

        // Test indexes serialized and deserialized
        List<Index> bIndexes = repository.getInnerIndexes(schemas[1]);

        for(Index sourceIndex: bIndexes){
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceIndex);
            Index deserializedIndex = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), Index.class);

            assertEquals(sourceIndex, deserializedIndex);
        }

        // Test triggers serialized and deserialized
        List<Trigger> cTriggers = repository.getInnerTriggers(schemas[2]);

        for(Trigger sourceTrigger: cTriggers){
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceTrigger);
            Trigger deserializedTrigger = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), Trigger.class);

            sourceTrigger.tidyUpProperties(sqlNormalizer);
            assertEquals(sourceTrigger, deserializedTrigger);
        }

        // Test routines serialized and deserialized
        List<Routine> aRoutines = repository.getInnerRoutines(schemas[0]);

        for(Routine sourceRoutine: aRoutines){
            InMemoryTestDBModelFS.SerializationInfo serializedInfo = outputFS.getSerializationInfo(sourceRoutine);
            Routine deserializedRoutine = textSerializer.fromYAMLtoPOJO(serializedInfo.getYamlText(), Routine.class);

            sourceRoutine.tidyUpProperties(sqlNormalizer);
            assertEquals(sourceRoutine, deserializedRoutine);
        }


    }

}