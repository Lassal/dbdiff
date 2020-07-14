package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.ColumnOrder;
import br.lassal.dbvcs.tatubola.relationaldb.model.Index;
import br.lassal.dbvcs.tatubola.relationaldb.model.IndexColumn;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class IndexSerializerTest {

    private final String rootPath = "root";

    @Mock
    private RelationalDBRepository repository;


    private InMemoryTestDBModelFS createNewDBModelFS(){
       return new InMemoryTestDBModelFS(this.rootPath, new JacksonYamlSerializer());
    }

    @Test
    public void testIndexSerializerSpecifSchema() throws Exception {
        String env = "DEV";
        String schema = "XPTO";
        InMemoryTestDBModelFS dbModelFS = this.createNewDBModelFS();
        IndexSerializer serializer = new IndexSerializer(this.repository, dbModelFS, schema, env);

        List<Index> unorderedIndexList = new ArrayList<>();
        unorderedIndexList.add(this.createTestIndex(schema, 2, false, true));
        unorderedIndexList.add(this.createTestIndex(schema, 1, true, true));

        List<Index> expectedIndexesOrdered = new ArrayList<>();
        expectedIndexesOrdered.add(this.createTestIndex(schema, 1, true, false));
        expectedIndexesOrdered.add(this.createTestIndex(schema, 2, false, false));

        when(this.repository.loadIndexes(schema)).thenReturn(unorderedIndexList);

        serializer.serialize();

        for (Index expectedIdx: expectedIndexesOrdered) {
            InMemoryTestDBModelFS.SerializationInfo serializationInfo = dbModelFS.getSerializationInfo(expectedIdx);
            assertEquals(expectedIdx, serializationInfo.getDBObject());
            //TODO: implement these tests in DBModelFS
            assertEquals(this.getIndexOutputPath((Index) serializationInfo.getDBObject()), serializationInfo.getObjectOutputPath());
        }

        verify(this.repository, times(1)).loadIndexes(schema);
        assertEquals(unorderedIndexList.size(), dbModelFS.getNumberSerializedObjects());
    }

    private Index createTestIndex(String schema, int id, boolean unique, boolean columnsOutOfOrder){
        String indexName = "Dummy_Idx0" + id;
        String targetTable = "Table_001";
        String indexType = "SOME_TYPE";

        Index idx = new Index(schema, indexName, schema, targetTable, indexType, unique);

        if(columnsOutOfOrder){
            idx.addColumn(this.createTestIndexColumn(3));
            idx.addColumn(this.createTestIndexColumn(5));
            idx.addColumn(this.createTestIndexColumn(1));
            idx.addColumn(this.createTestIndexColumn(2));
            idx.addColumn(this.createTestIndexColumn(4));
        }
        else{
            for(int i =1; i < 6; i++){
                idx.addColumn(this.createTestIndexColumn(i));
            }
        }


        return idx;
    }

    private IndexColumn createTestIndexColumn(int seqId){
        String columnName = "IDX_COL_" + seqId;
        ColumnOrder order = seqId % 2 == 0 ? ColumnOrder.ASC : ColumnOrder.DESC;
        IndexColumn col = new IndexColumn(columnName, seqId, order);

        return col;
    }

    //TODO: remove from here; implement in the DBModelFS unit tests
    private Path getIndexOutputPath(Index index){
        return Paths.get( this.rootPath,
                String.format("%s/Tables/Indexes/TABLE_%s_INDEX_%s.yaml"
                        , index.getAssociateTableSchema().toUpperCase()
                        , index.getAssociateTableName(), index.getName()
                )
        );
    }
}