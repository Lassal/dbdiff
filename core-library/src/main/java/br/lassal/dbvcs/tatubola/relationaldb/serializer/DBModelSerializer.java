package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

public abstract class DBModelSerializer<M extends DatabaseModelEntity> {

    private RelationalDBRepository repository;
    private String schema;
    private DBModelFS modelFS;
    private List<LoadCommand> loadSteps;
    private String environmentName;
    private Logger logger;

    public DBModelSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName){
        this.repository = repository;
        this.modelFS = dbModelFS;
        this.schema = targetSchema;
        this.loadSteps = new ArrayList<>();
        this.environmentName = environmentName;

        this.defineLoadSteps();
    }

    protected void setLogger(Logger logger){
        this.logger = logger;
    }

    protected RelationalDBRepository getRepository() {
        return repository;
    }

    protected String getSchema() {
        return schema;
    }

    protected DBModelFS getModelFS() {
        return modelFS;
    }

    public String getEnvironmentName(){ return this.environmentName; }

    abstract List<M> assemble();

    void serialize(List<M> entities) throws Exception {
        for(M model : entities){
            this.getModelFS().save(model);
        }
    }

    protected long showEllapsedMicros(long start){
        double delta = (System.nanoTime() - start) / 1000000.00;
        System.out.print("Ellapsed millis " + delta + " \n");

        return System.nanoTime();
    }

    protected void addLoadStep(LoadCommand loadStep){
        this.loadSteps.add(loadStep);
    }

    public List<LoadCommand> getLoadSteps(){
        return this.loadSteps;
    }

    protected abstract void defineLoadSteps();

    public void serialize() throws Exception {

        long lastMark = System.nanoTime();

        for(LoadCommand loadStep : this.loadSteps){
            loadStep.execute();
            lastMark = this.showEllapsedMicros(lastMark);
        }

        this.serialize(this.assemble());

        lastMark = this.showEllapsedMicros(lastMark);

    }

    protected void trace(String action, String message){
        if(this.logger != null && this.logger.isTraceEnabled()){
            this.logger.trace(String.format("Env: %s|Schema: %s|Action: %s > %s"
                    , this.getEnvironmentName(), this.getSchema(), action, message));
        }
    }

}
