package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics.SerializerMetricsListener;
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
    private SerializerMetricsListener metricListener;

    public DBModelSerializer(RelationalDBRepository repository, DBModelFS dbModelFS
            , String targetSchema, String environmentName, Logger logger) {
        this.repository = repository;
        this.modelFS = dbModelFS;
        this.schema = targetSchema;
        this.loadSteps = new ArrayList<>();
        this.environmentName = environmentName;
        this.logger = logger;

        this.defineLoadSteps();
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

    public String getEnvironmentName() {
        return this.environmentName;
    }

    abstract List<M> assemble();

    void serialize(List<M> entities) throws Exception {
        for (M model : entities) {
            this.getModelFS().save(model);
        }

        this.notifyAfterSerialization(entities);
    }

    private void notifyAfterSerialization(List<M> entities){

        if(this.metricListener != null && !entities.isEmpty()){
            this.metricListener.notifySerializedObjects(entities.get(0).getClass(), this.schema, entities.size());
        }
    }

    public DBModelSerializer setMetricsListener(SerializerMetricsListener listener){
        this.metricListener = listener;

        return this;
    }

    protected long showEllapsedMicros(long start) {
        double delta = (System.nanoTime() - start) / 1000000.00;
        this.logger.trace("Ellapsed millis " + delta);

        return System.nanoTime();
    }

    protected void addLoadStep(LoadCommand loadStep, String stepName) {

        this.loadSteps.add(this.traceLoadStep(loadStep, stepName));

    }

    private LoadCommand traceLoadStep(LoadCommand loadStep, String stepName) {
        String logMessage = String.format("Env: %s|Schema: %s|Action: %s > "
                , this.getEnvironmentName(), this.getSchema(), stepName);

        if (this.logger != null && this.logger.isTraceEnabled()) {
            return () -> {
                this.logger.trace(logMessage + " BEFORE ACTION");
                loadStep.execute();
                this.logger.trace(logMessage + " AFTER ACTION");
            };
        } else {
            return loadStep;
        }
    }

    public List<LoadCommand> getLoadSteps() {
        return this.loadSteps;
    }

    protected abstract void defineLoadSteps();

    public void serialize() throws Exception {

        long lastMark = System.nanoTime();

        for (LoadCommand loadStep : this.loadSteps) {
            loadStep.execute();
            lastMark = this.showEllapsedMicros(lastMark);
        }

        this.serialize(this.assemble());

        this.showEllapsedMicros(lastMark);

    }

    protected void trace(String action, String message) {
        if (this.logger != null && this.logger.isTraceEnabled()) {
            this.logger.trace(String.format("Env: %s|Schema: %s|Action: %s > %s"
                    , this.getEnvironmentName(), this.getSchema(), action, message));
        }
    }

}
