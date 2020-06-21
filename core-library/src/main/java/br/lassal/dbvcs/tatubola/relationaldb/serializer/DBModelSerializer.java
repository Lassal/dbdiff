package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;

import java.util.ArrayList;
import java.util.List;

public abstract class DBModelSerializer<M extends DatabaseModelEntity> {

    private RelationalDBRepository repository;
    private String schema;
    private DBModelFS output;
    private List<LoadCommand> loadSteps;

    public DBModelSerializer(RelationalDBRepository repository, String targetSchema, String outputPath){
        this.repository = repository;
        this.output = new DBModelFS(outputPath, new JacksonYamlSerializer());
        this.schema = targetSchema;
        this.loadSteps = new ArrayList<>();

        this.defineLoadSteps();
    }

    protected RelationalDBRepository getRepository() {
        return repository;
    }

    protected String getSchema() {
        return schema;
    }

    protected DBModelFS getOutput() {
        return output;
    }

    abstract List<M> assemble();

    void serialize(List<M> entities) throws Exception {
        for(M model : entities){
            this.getOutput().save(model);
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

}
