package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class ParallelSerializer<S extends DBModelSerializer> extends RecursiveAction {

    private DBModelSerializer serializer;

    public ParallelSerializer(DBModelSerializer serializer){
        this.serializer = serializer;
    }

    @Override
    protected void compute() {
        List<RecursiveAction> loadAction = new ArrayList<>();

        List<LoadCommand> loadSteps = this.serializer.getLoadSteps();

        for(LoadCommand loadStep : loadSteps)
            loadAction.add(this.convertToRecursiveAction(loadStep));

        this.invokeAll(loadAction);

        List<DatabaseModelEntity> dbEntities = this.serializer.assemble();

        try {
            this.serializer.serialize(dbEntities);
        } catch (Exception e) {
            e.printStackTrace();
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
