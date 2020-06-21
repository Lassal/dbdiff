package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;

import java.util.List;

public class TriggerSerializer extends DBModelSerializer<Trigger>{

    private List<Trigger> triggers;

    public TriggerSerializer(MySQLRepository repository, String targetSchema, String outputPath){
        super(repository, targetSchema, outputPath);
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
