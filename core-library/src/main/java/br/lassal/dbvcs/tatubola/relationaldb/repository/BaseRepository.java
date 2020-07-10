package br.lassal.dbvcs.tatubola.relationaldb.repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class BaseRepository implements RelationalDBRepository {

    private DataSource dataSource;

    public BaseRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }
}
