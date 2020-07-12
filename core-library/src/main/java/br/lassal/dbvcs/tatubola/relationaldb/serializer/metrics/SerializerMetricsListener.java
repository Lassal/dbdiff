package br.lassal.dbvcs.tatubola.relationaldb.serializer.metrics;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;

public interface SerializerMetricsListener {

    void notifySerializedObjects(Class<? extends DatabaseModelEntity> dbObjectType, String schema, int numberSerializedObjects);
}
