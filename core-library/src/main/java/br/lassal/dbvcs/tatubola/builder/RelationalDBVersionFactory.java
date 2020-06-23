package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public class RelationalDBVersionFactory {

    public DataSource getConnectionPool(String jdbcUrl, String username, String password
            , int minPoolSize, int maxPoolSize, boolean openConnectionsOnInitialization) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:9501/classicmodels");
        config.setUsername("local-admin");
        config.setPassword("db12345");
        config.setMinimumIdle(minPoolSize);
        config.setMaximumPoolSize(maxPoolSize);
        config.setInitializationFailTimeout(openConnectionsOnInitialization ? 1 : -1);
        //  config.addDataSourceProperty( "cachePrepStmts" , "true" );
        //  config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        //  config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        return new HikariDataSource(config);
    }

    public DataSource getConnectionPool(String jdbcUrl, String username, String password, boolean openConnectionsOnInitialization) {
        return this.getConnectionPool(jdbcUrl, username, password, 4, 16, true); //4, 16
    }

    public DataSource getConnectionPool(String jdbcUrl, String username, String password) {
        return this.getConnectionPool(jdbcUrl, username, password, true); //4, 16
    }



    public RelationalDBRepository getRDBRepository(String jdbcUrl, String username, String password, boolean openConnectionsOnInitialization){

        if(jdbcUrl != null && !jdbcUrl.isEmpty()){
            String[] jdbcUrlInfo = jdbcUrl.split(":");

            if(jdbcUrlInfo.length > 2){
                return DatabaseVendor.createRelationalDBRepository(jdbcUrlInfo[1]
                        , this.getConnectionPool(jdbcUrl, username, password, openConnectionsOnInitialization));
            }
        }
        return null;
    }

    public RelationalDBRepository getRDBRepository(String jdbcUrl, String username, String password){
        return this.getRDBRepository(jdbcUrl, username, password, true);
    }
}
