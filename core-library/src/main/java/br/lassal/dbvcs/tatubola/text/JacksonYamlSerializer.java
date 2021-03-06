package br.lassal.dbvcs.tatubola.text;

import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Path;


public class JacksonYamlSerializer implements TextSerializer {

    private ObjectMapper yamlMapper;

    public JacksonYamlSerializer() {
        this.yamlMapper = new ObjectMapper(new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
        this.yamlMapper.findAndRegisterModules();
        this.yamlMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    private <W extends Writer> W writeAsYAML(W output, DatabaseModelEntity dbEntity) throws IOException {
        this.yamlMapper.writeValue(output, dbEntity);
        return output;
    }

    @Override
    public String toText(DatabaseModelEntity entity) throws IOException {
        StringWriter output = new StringWriter();
        this.writeAsYAML(output, entity);
        return output.toString();
    }

    @Override
    public <T> T fromYAMLtoPOJO(Path yamlPath, Class<T> outpuClass) throws IOException {
        return this.yamlMapper.readValue(yamlPath.toFile(), outpuClass);
    }

    @Override
    public <T> T fromYAMLtoPOJO(String yaml, Class<T> outpuClass) throws IOException {
        return this.yamlMapper.readValue(yaml, outpuClass);
    }
}
