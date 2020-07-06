package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class ParallelSerializer<S extends DBModelSerializer> extends RecursiveAction {

    private static Logger logger = LoggerFactory.getLogger(ParallelSerializer.class);

    private DBModelSerializer serializer;

    public ParallelSerializer(DBModelSerializer serializer){
        this.serializer = serializer;
    }

    @Override
    protected void compute() {
        List<RecursiveAction> loadActions = new ArrayList<>();

        List<LoadCommand> loadSteps = this.serializer.getLoadSteps();

        for(LoadCommand loadStep : loadSteps)
            loadActions.add(this.convertToRecursiveAction(loadStep));

        String logPrefix = null;

        if(logger.isDebugEnabled()){
           logPrefix = String.format("Serializer %s [ENV: %s | Schema: %s] : "
                       , this.serializer.getClass().getSimpleName(), this.serializer.getEnvironmentName()
                       , this.serializer.getSchema());

            logger.debug(logPrefix + "(A) call load DB metadata actions");

        }
        this.invokeAll(loadActions);

        if(logger.isDebugEnabled()){
            logger.debug(logPrefix + "(B) finished load DB metadata");
        }


        List<DatabaseModelEntity> dbEntities = this.serializer.assemble();

        try {
            this.serializer.serialize(dbEntities);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if(logger.isDebugEnabled()){
            logger.debug(logPrefix + "(C) all objects serialized to filesystem");

        }

    }

    private RecursiveAction convertToRecursiveAction(LoadCommand command){
        return new RecursiveAction() {
            @Override
            protected void compute() {
                command.execute();
            }
        };
    }
}
