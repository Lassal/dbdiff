package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class ParallelSerializer<S extends DBModelSerializer> extends RecursiveAction {

    private static Logger logger = LoggerFactory.getLogger(ParallelSerializer.class);

    private transient DBModelSerializer serializer;
    private transient CountDownLatch allTaskCounter;

    public ParallelSerializer(DBModelSerializer serializer, CountDownLatch allTaskCounter) {
        this.serializer = serializer;
        this.allTaskCounter = allTaskCounter;
    }

    @Override
    protected void compute() {

        String logPrefix = null;

        if (logger.isDebugEnabled()) {
            logPrefix = String.format("Serializer %s [ENV: %s | Schema: %s] : "
                    , this.serializer.getClass().getSimpleName(), this.serializer.getEnvironmentName()
                    , this.serializer.getSchema());

            logger.debug(logPrefix + "(A) call load DB metadata actions");

        }

        try{
            List<RecursiveAction> loadActions = new ArrayList<>();

            List<LoadCommand> loadSteps = this.serializer.getLoadSteps();

            for (LoadCommand loadStep : loadSteps)
                loadActions.add(this.convertToRecursiveAction(loadStep));

            ForkJoinTask.invokeAll(loadActions);

            if (logger.isDebugEnabled()) {
                logger.debug(logPrefix + "(B) finished load DB metadata");
            }


            List<DatabaseModelEntity> dbEntities = this.serializer.assemble();

            try {
                this.serializer.serialize(dbEntities);
            } catch (Exception e) {
                logger.error("Error serializing db entities", e);
            }

            if (logger.isDebugEnabled()) {
                logger.debug(logPrefix + "(C) all objects serialized to filesystem");

            }


        }catch (Throwable ex){
            this.completeExceptionally(ex);
            logger.error(
                    String.format("An error was encountered serializing the objects in ENV: %s | SCHEMA: %s"
                            , this.serializer.getEnvironmentName()
                            , this.serializer.getSchema()), ex);
        }
        finally {
            this.allTaskCounter.countDown();
            if (logger.isDebugEnabled()) {
                logger.debug(logPrefix + "(C2) remaining tasks #" + this.allTaskCounter.getCount());

            }

        }

    }

    private RecursiveAction convertToRecursiveAction(LoadCommand command) {
        return new RecursiveAction() {
            @Override
            protected void compute(){
                command.execute();
            }
        };
    }


}
