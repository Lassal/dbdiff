package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.relationaldb.repository.BaseRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.OracleRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum DatabaseRepositoryVendor {
    MYSQL,
    ORACLE;

    private static Logger logger = LoggerFactory.getLogger(DatabaseRepositoryVendor.class);


    public static BaseRepository createRelationalDBRepository(String dbVendor) {

        DatabaseRepositoryVendor database = null;

        try {
            database = DatabaseRepositoryVendor.valueOf(dbVendor.toUpperCase());
        } catch (IllegalArgumentException ex) {
            logger.warn("Could not identify database : " + dbVendor, ex);
        }


        if (database == null) {
            return null;
        }

        switch (database) {
            case MYSQL:
                return new MySQLRepository();
            case ORACLE:
                return new OracleRepository();
            default:
                return null;
        }
    }
}
