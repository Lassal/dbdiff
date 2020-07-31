package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Trigger;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TriggerSerializer extends DBModelSerializer<Trigger> {

    private static Logger logger = LoggerFactory.getLogger(TriggerSerializer.class);

    private List<Trigger> triggers;

    public TriggerSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName) {
        super(repository, dbModelFS, targetSchema, environmentName, TriggerSerializer.logger);
    }


    List<Trigger> assemble() {

        if(this.triggers != null){
            return this.triggers;
        }
        else{
            logger.warn(String.format("The repository return NULL for the triggers in Environment: %s | Schema: %s",
                    this.getEnvironmentName(), this.getSchema()));

            return new ArrayList<>();
        }
    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadTriggersStep(), "Load Triggers");
    }

    private LoadCommand getLoadTriggersStep() {
        TriggerSerializer serializer = this;

        return () -> serializer.triggers = serializer.getRepository().loadTriggers(serializer.getSchema());
    }

}
