package br.lassal.dbvcs.tatubola.fs;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;

import java.nio.file.Path;

public interface DBModelFS {

    Path save(DatabaseModelEntity dbEntity) throws Exception;
}
