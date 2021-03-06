package br.lassal.dbvcs.tatubola.fs;


import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import br.lassal.dbvcs.tatubola.text.TextSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class InMemoryTestDBModelFS extends BaseDBModelFS{

    private static Logger logger = LoggerFactory.getLogger(InMemoryTestDBModelFS.class);

    public static String getObjectID(DatabaseModelEntity object){
        return object.getSchema() + "." + object.getName();
    }

    public class SerializationInfo {
        private String objectId;
        private DatabaseModelEntity dbObject;
        private Path objectOutputPath;
        private String yamlText;

        public SerializationInfo(DatabaseModelEntity dbObject, Path objectOutputPath){
            this.dbObject = dbObject;
            this.objectOutputPath = objectOutputPath;
            this.objectId = InMemoryTestDBModelFS.getObjectID(dbObject);
        }

        public String getObjectId(){
            return this.objectId;
        }

        public Path getObjectOutputPath(){
            return this.objectOutputPath;
        }

        public DatabaseModelEntity getDBObject(){
            return this.dbObject;
        }

        public void setYamlText(String yamlText){
            this.yamlText = yamlText;
        }

        public String getYamlText(){
            return this.yamlText;
        }
    }

    private Map<String, SerializationInfo> serializedObjects;

    public InMemoryTestDBModelFS(String rootPath, TextSerializer textSerializer) {
        super(rootPath, textSerializer);
        this.serializedObjects = new HashMap<>();
    }

    @Override
    protected void safeWrite(Path dbModelFile, DatabaseModelEntity dbEntity) throws DBModelFSException, IOException {
        String objID = InMemoryTestDBModelFS.getObjectID(dbEntity);

        SerializationInfo serInfo = new SerializationInfo(dbEntity, dbModelFile);
        serInfo.setYamlText(this.getObjToTxtTranslator().toText(dbEntity));

        logger.info("Serialized object: " + objID + "\n-------------------\n" + serInfo.getYamlText());

        this.serializedObjects.put(objID, serInfo);
    }

    public SerializationInfo getSerializationInfo(DatabaseModelEntity dbObject){
        String id = InMemoryTestDBModelFS.getObjectID(dbObject);

        return this.serializedObjects.get(id);
    }

    public int getNumberSerializedObjects(){
        return this.serializedObjects.values().size();
    }
}