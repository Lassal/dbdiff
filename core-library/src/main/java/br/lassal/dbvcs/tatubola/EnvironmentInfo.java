package br.lassal.dbvcs.tatubola;

import br.lassal.dbvcs.tatubola.builder.DBModelSerializerBuilder;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.ParallelSerializer;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics.SerializerCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class EnvironmentInfo {

    private DBModelSerializerBuilder dbEnvironment;
    private CountDownLatch taskSerializerLatch;
    private SerializerCounter serializerCounter;
    private List<ParallelSerializer<? extends DBModelSerializer>> parallelSerializers;

    public EnvironmentInfo(DBModelSerializerBuilder dbEnvironment, int numberOfTaskToSerialize){
        this.dbEnvironment = dbEnvironment;
        this.taskSerializerLatch = new CountDownLatch(numberOfTaskToSerialize);
        this.serializerCounter = new SerializerCounter(dbEnvironment.getEnvironmentName());
        this.parallelSerializers = new ArrayList<>();
    }

    public CountDownLatch getTaskSerializerLatch(){
        return this.taskSerializerLatch;
    }

    public String getEnvName(){
        return this.dbEnvironment.getEnvironmentName();
    }

    public String getOutputPath(){
        return this.dbEnvironment.getOutputPath();
    }

    public String getDBJdbcUrl(){
        return this.dbEnvironment.getJdbcUrl();
    }

    public SerializerCounter getSerializerCounter(){
        return this.serializerCounter;
    }

    public ParallelSerializer<? extends DBModelSerializer> addParallelSerializer(ParallelSerializer<? extends DBModelSerializer> serializer){
        this.parallelSerializers.add(serializer);

        return serializer;
    }

    public List<ParallelSerializer<? extends DBModelSerializer>> getParallelSerializers(){
        return this.parallelSerializers;
    }

    public void checkFailedSerializers() throws Throwable {
        for(ParallelSerializer ser : this.parallelSerializers){
            if(ser.isCompletedAbnormally()){
                throw ser.getException();
            }
        }
    }

}
