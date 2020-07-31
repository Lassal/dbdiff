package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Index;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IndexSerializer extends DBModelSerializer<Index> {

    private static Logger logger = LoggerFactory.getLogger(IndexSerializer.class);

    private List<Index> indexes;

    public IndexSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName) {
        super(repository, dbModelFS, targetSchema, environmentName, logger);
    }


    List<Index> assemble() {

        if(this.indexes != null){
            return this.indexes;
        }
        else{
            logger.warn(String.format("The repository return NULL for the indexes in Environment: %s | Schema: %s",
                    this.getEnvironmentName(), this.getSchema()));

            return new ArrayList<>();
        }
    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadIndexes(), "LoadIndexes");
    }

    private LoadCommand getLoadIndexes() {
        IndexSerializer serializer = this;

        return () -> serializer.indexes = serializer.getRepository().loadIndexes(serializer.getSchema());
    }


}
