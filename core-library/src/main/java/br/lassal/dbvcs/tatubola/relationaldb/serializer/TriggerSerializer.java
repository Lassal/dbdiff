package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;

import java.util.List;

public class TriggerSerializer extends DBModelSerializer<Trigger>{

    private List<Trigger> triggers;

    public TriggerSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema){
        super(repository, dbModelFS, targetSchema );
    }


    List<Trigger> assemble(){
        return this.triggers;
    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadTriggersStep());
    }

    private LoadCommand getLoadTriggersStep(){
        TriggerSerializer serializer = this;

        return () -> serializer.triggers = serializer.getRepository().loadTriggers(serializer.getSchema());
    }

}
