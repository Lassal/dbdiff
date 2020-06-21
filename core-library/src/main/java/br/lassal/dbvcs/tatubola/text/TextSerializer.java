package br.lassal.dbvcs.tatubola.text;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;

import java.io.IOException;

public interface TextSerializer {

    String toText(DatabaseModelEntity entity) throws IOException;
}
