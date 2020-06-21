package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.Index;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;

import java.util.List;

public class IndexSerializer extends  DBModelSerializer<Index> {

    private List<Index> indexes;

    public IndexSerializer(RelationalDBRepository repository, String targetSchema, String outputPath){
        super(repository, targetSchema, outputPath);
    }


    List<Index> assemble(){
        return this.indexes;
    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadIndexes());
    }

    private LoadCommand getLoadIndexes(){
        IndexSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.indexes = serializer.getRepository().loadIndexes(serializer.getSchema());
            }
        };
    }


}
