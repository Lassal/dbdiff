package br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SerializerCounter implements SerializerMetricsListener{

    private static Logger logger = LoggerFactory.getLogger(SerializerCounter.class);

    private String environmentName;
    private Map<String, SchemaMetrics> schemaStats;

    public SerializerCounter(String environmentName){
        this.environmentName = environmentName;
        this.schemaStats = new HashMap<>();
    }

    public List<SchemaMetrics> getMetrics(){
        return this.schemaStats.values().stream()
                .sorted(Comparator.comparing(SchemaMetrics::getSchema))
                .collect(Collectors.toList());
    }

    @Override
    public void notifySerializedObjects(Class<? extends DatabaseModelEntity> dbObjectType, String schema, int numberSerializedObjects) {

        SchemaMetrics metrics = null;
        if(this.schemaStats.containsKey(schema)){
            metrics = this.schemaStats.get(schema);
        }
        else{
            metrics = new SchemaMetrics(schema);
            this.schemaStats.put(schema, metrics);
        }

        metrics.increment(dbObjectType, numberSerializedObjects);
    }


}
