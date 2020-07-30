package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import br.lassal.dbvcs.tatubola.relationaldb.model.TriggerDummyBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TriggerSerializerTest extends BaseSerializerTest{


    /**
     * Test how the serialization process works for Triggers
     * Checks how the full process works:
     *   - assemble (a single method here)
     *   - serialization
     *   - tidy up properties : in this case manual verification
     * @throws Exception
     */
    @Test
    public void testTriggerSerializer() throws Exception {
        String env = "DEV";
        String schema = "ABC";

        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();
        TriggerDummyBuilder triggerBuilder = new TriggerDummyBuilder();

        TriggerSerializer serializer = new TriggerSerializer(this.repository, dbModelFS, schema, env);

        List<Trigger> sourceTriggers = new ArrayList<>();
        sourceTriggers.add(triggerBuilder.createSampleTrigger(5, schema, "TB_FIRST"));
        sourceTriggers.add(triggerBuilder.createSampleTrigger(1, schema, "TB_FIFTH"));
        sourceTriggers.add(triggerBuilder.createSampleTrigger(3, schema, "TB_THIRD"));
        sourceTriggers.add(triggerBuilder.createSampleTrigger(4, schema, "TB_FOURTH"));
        sourceTriggers.add(triggerBuilder.createSampleTrigger(2, schema, "TB_SECOND"));

        when(this.repository.loadTriggers(schema)).thenReturn(sourceTriggers);

        serializer.serialize();

        for(Trigger sourceTrigger: sourceTriggers){
            InMemoryTestDBModelFS.SerializationInfo serializationInfo = dbModelFS.getSerializationInfo(sourceTrigger);

            assertEquals(sourceTrigger, serializationInfo.getDBObject());
        }

        verify(this.repository, times(1)).loadTriggers(schema);
        assertTrue(dbModelFS.getNumberSerializedObjects() > 2);
    }

    /**
     * Test if the deserialized version of a Trigger in textual format is identical to the
     * original Trigger
     * @throws Exception
     */
    @Test
    public void testSerializeDeserializeTrigger() throws Exception {
        String env = "DEV";
        String schema = "ABC";

        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();
        TriggerDummyBuilder triggerBuilder = new TriggerDummyBuilder();
        TriggerSerializer serializer = new TriggerSerializer(this.repository, dbModelFS, schema, env);

        List<Trigger> sourceTriggers = new ArrayList<>();
        sourceTriggers.add(triggerBuilder.createSampleTrigger(1, schema, "TB_FIRST"));

        when(this.repository.loadTriggers(schema)).thenReturn(sourceTriggers);
        serializer.serialize();

        Trigger sourceTrigger = sourceTriggers.get(0);
        InMemoryTestDBModelFS.SerializationInfo serializationInfo = dbModelFS.getSerializationInfo(sourceTrigger);

        assertEquals(sourceTrigger, serializationInfo.getDBObject());

        Trigger deserializedTrigger = this.textSerializer.fromYAMLtoPOJO(serializationInfo.getYamlText(), Trigger.class);

        assertEquals(sourceTrigger, deserializedTrigger);

    }

}