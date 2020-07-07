package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TriggerSerializer extends DBModelSerializer<Trigger>{

    private static Logger logger = LoggerFactory.getLogger(TriggerSerializer.class);

    private List<Trigger> triggers;

    public TriggerSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName){
        super(repository, dbModelFS, targetSchema, environmentName, TriggerSerializer.logger );
    }


    List<Trigger> assemble(){
        return this.triggers;
    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadTriggersStep(), "Load Triggers");
    }

    private LoadCommand getLoadTriggersStep(){
        TriggerSerializer serializer = this;

        return () -> {
            serializer.triggers = serializer.getRepository().loadTriggers(serializer.getSchema());
        };

    }

}
