package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.text.SqlNormalizer;

public interface DatabaseModelEntity {

    String getSchema();

    String getName();

    /**
     * This method should implement all the code need to order, organize and
     * normalize all entity properties before it is serialized
     */
    void tidyUpProperties(SqlNormalizer normalizer);

}
