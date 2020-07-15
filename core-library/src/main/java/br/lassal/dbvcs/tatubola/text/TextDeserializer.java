package br.lassal.dbvcs.tatubola.text;

import java.io.IOException;
import java.nio.file.Path;

public interface TextDeserializer {

    <T> T fromYAMLtoPOJO(Path yamlPath, Class<T> outpuClass) throws IOException;

    <T> T fromYAMLtoPOJO(String yaml, Class<T> outpuClass) throws IOException;
}
