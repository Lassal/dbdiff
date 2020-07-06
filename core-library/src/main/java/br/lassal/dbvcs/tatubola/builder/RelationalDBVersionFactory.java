package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.fs.BaseDBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.DatabaseModelEntity;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import br.lassal.dbvcs.tatubola.relationaldb.serializer.*;
import br.lassal.dbvcs.tatubola.text.JacksonYamlSerializer;
import br.lassal.dbvcs.tatubola.text.TextSerializer;
import br.lassal.dbvcs.tatubola.versioncontrol.GitController;
import br.lassal.dbvcs.tatubola.versioncontrol.VersionControlSystem;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class RelationalDBVersionFactory {

    private static RelationalDBVersionFactory instance = new RelationalDBVersionFactory();

    public static RelationalDBVersionFactory getInstance(){
        return instance;
    }

    private RelationalDBVersionFactory(){

    }

    public DataSource createConnectionPool(String jdbcUrl, String username, String password
            , int minPoolSize, int maxPoolSize, boolean openConnectionsOnInitialization) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMinimumIdle(minPoolSize);
        config.setMaximumPoolSize(maxPoolSize);
        config.setInitializationFailTimeout(openConnectionsOnInitialization ? 1 : -1);
        //  config.addDataSourceProperty( "cachePrepStmts" , "true" );
        //  config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        //  config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        return new HikariDataSource(config);
    }

    public DataSource createConnectionPool(String jdbcUrl, String username, String password
            , boolean openConnectionsOnInitialization) {
        return this.createConnectionPool(jdbcUrl, username, password, 4, 16, openConnectionsOnInitialization); //4, 16
    }

    public DataSource createConnectionPool(String jdbcUrl, String username, String password) {
        return this.createConnectionPool(jdbcUrl, username, password, true); //4, 16
    }


    public RelationalDBRepository createRDBRepository(String jdbcUrl, String username, String password, boolean openConnectionsOnInitialization) {

        if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
            String[] jdbcUrlInfo = jdbcUrl.split(":");

            if (jdbcUrlInfo.length > 2) {
                return DatabaseRepositoryVendor.createRelationalDBRepository(jdbcUrlInfo[1]
                        , this.createConnectionPool(jdbcUrl, username, password, openConnectionsOnInitialization));
            }
        }
        return null;
    }

    public RelationalDBRepository createRDBRepository(String jdbcUrl, String username, String password) {
        return this.createRDBRepository(jdbcUrl, username, password, true);
    }

    public DBModelFS createDBModelFS(String rootPathPerEnv) {
        TextSerializer serializer = new JacksonYamlSerializer();

        return new BaseDBModelFS(rootPathPerEnv, serializer);
    }

    public List<DBModelSerializer> createDBObjectsSerializers(
            String environmentName, String schema, RelationalDBRepository repository, DBModelFS dbModelFS) {

        List<DBModelSerializer> serializers = new ArrayList<>();
        serializers.add(new TableSerializer(repository, dbModelFS, schema, environmentName));
        serializers.add(new RoutineSerializer(repository, dbModelFS, schema, environmentName));
        serializers.add(new ViewSerializer(repository, dbModelFS, schema, environmentName));
        serializers.add(new TriggerSerializer(repository, dbModelFS, schema, environmentName));
        serializers.add(new IndexSerializer(repository, dbModelFS, schema, environmentName));


        return serializers;
    }

    public List<DBModelSerializer> createDBObjectsSerializers(String environmentName, String schema, String jdbcUrl
            , String username, String password, String outputPath, boolean openConnectionsOnInitialization) {

        RelationalDBRepository repository = this.createRDBRepository(jdbcUrl, username, password, openConnectionsOnInitialization);
        DBModelFS dbModelFS = this.createDBModelFS(outputPath);

        return this.createDBObjectsSerializers(environmentName ,schema, repository, dbModelFS);
    }

    public List<DBModelSerializer> createDBObjectsSerializers(String environmentName, String schema, String jdbcUrl
            , String username, String password, String outputPath) {

        return this.createDBObjectsSerializers(environmentName, schema, jdbcUrl, username, password, outputPath, true);

    }

    public List<RecursiveAction> createParallelDBObjectsSerializers(String environmentName, String schema, String jdbcUrl
            , String username, String password, String outputPath, boolean openConnectionsOnInitialization){

        List<DBModelSerializer> serializers = this.createDBObjectsSerializers(environmentName, schema, jdbcUrl, username, password,
                    outputPath );

        List<RecursiveAction> parallelSerializer = new ArrayList<>();

        for (DBModelSerializer ser: serializers) {
            parallelSerializer.add(new ParallelSerializer(ser));
        }

        return parallelSerializer;
    }

    public VersionControlSystem createVCSController(String repositoryUrl, String username, String password, String baseBranch)
            throws MalformedURLException {
        URL remoteRepositoryURL = new URL(repositoryUrl);
        GitController vcs = new GitController(remoteRepositoryURL, username, password);
        vcs.setBaseBranch(baseBranch);

        return vcs;
    }

}
