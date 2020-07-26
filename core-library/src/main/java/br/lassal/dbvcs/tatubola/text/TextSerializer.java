package br.lassal.dbvcs.tatubola.text;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;

import java.io.IOException;
import java.nio.file.Path;

public interface TextSerializer {

    String toText(DatabaseModelEntity entity) throws IOException;

    <T> T fromYAMLtoPOJO(Path yamlPath, Class<T> outpuClass) throws IOException;

    <T> T fromYAMLtoPOJO(String yaml, Class<T> outpuClass) throws IOException;

}
