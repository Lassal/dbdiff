package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.fs.FSManager;
import br.lassal.dbvcs.tatubola.fs.InMemoryTestDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.DBModelSerializer;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import br.lassal.dbvcs.tatubola.text.TextSerializer;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;

import static org.mockito.Mockito.*;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockDatabaseSerializerFactory implements DatabaseSerializerFactory{

    private Map<String, InMemoryTestDBModelFS> outputDBModelFS = new HashMap<>();
    private VersionControlSystem vcs;
    private RelationalDBRepository repository;

    public VersionControlSystem getVcs() {
        return vcs;
    }

    public void setVcs(VersionControlSystem vcs) {
        this.vcs = vcs;
    }

    public RelationalDBRepository getRepository() {
        return repository;
    }

    public void setRepository(RelationalDBRepository repository) {
        this.repository = repository;
    }

    @Override
    public RelationalDBRepository createRDBRepository(String jdbcUrl, String username, String password) {
        return this.getRepository();
    }

    @Override
    public DBModelFS createDBModelFS(String rootPathPerEnv) {
        InMemoryTestDBModelFS dbModelFS;

        if(this.outputDBModelFS.containsKey(rootPathPerEnv)){
            dbModelFS = this.outputDBModelFS.get(rootPathPerEnv);
        }
        else{
            TextSerializer serializer = new JacksonYamlSerializer();
            dbModelFS = new InMemoryTestDBModelFS(rootPathPerEnv, serializer);
            this.outputDBModelFS.put(rootPathPerEnv, dbModelFS);
        }

        return dbModelFS;
    }

    public InMemoryTestDBModelFS getDBModelFS(String rootPathPerEnv){
        return this.outputDBModelFS.get(rootPathPerEnv);
    }

    @Override
    public VersionControlSystem createVCSController(String repositoryUrl, String username, String password, String baseBranch) throws MalformedURLException {
        return this.getVcs();
    }

    @Override
    public List<DBModelSerializer> createDBObjectsSerializers(String environmentName, String schema, RelationalDBRepository repository, DBModelFS dbModelFS) {
        return RelationalDBVersionFactory.getInstance()
                .createDBObjectsSerializers(environmentName, schema, repository, dbModelFS);
    }

    @Override
    public FSManager getFSManager() {
        return mock(FSManager.class);
    }

}
