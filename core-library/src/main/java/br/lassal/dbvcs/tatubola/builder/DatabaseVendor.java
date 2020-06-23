package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.OracleRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

public enum DatabaseVendor {
    MYSQL,
    ORACLE;

    private static Logger logger = LoggerFactory.getLogger(DatabaseVendor.class);


    public static RelationalDBRepository createRelationalDBRepository(String dbVendor, DataSource datasource){

        DatabaseVendor database = null;

        try{
            database = DatabaseVendor.valueOf(dbVendor.toUpperCase());
        }
        catch (IllegalArgumentException ex){
            logger.warn("Could not identify database : " + dbVendor, ex);
        }


        if(database == null){
            return null;
        }

        switch (database){
            case MYSQL: return new MySQLRepository(datasource);
            case ORACLE: return new OracleRepository(datasource);
            default: return null;
        }
    }
}
