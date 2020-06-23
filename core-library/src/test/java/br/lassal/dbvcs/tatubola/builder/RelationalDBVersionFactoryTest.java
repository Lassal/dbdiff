package br.lassal.dbvcs.tatubola.builder;

import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.OracleRepository;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.junit.Test;

import static org.junit.Assert.*;

public class RelationalDBVersionFactoryTest {

    @Test
    public void testCreateRelationalDBRepository(){
        String mySqlUrl = "jdbc:mysql://localhost:3306/test";
        String oracleUrl = "jdbc:oracle:thin:root/secret@localhost:1521:testdb";
        String postreSQLUrl = "jdbc:postgresql://localhost:5432/testdb";
        boolean initializeConnPool = false;

        RelationalDBVersionFactory factory = new RelationalDBVersionFactory();

        RelationalDBRepository mySqlRepo = factory.createRDBRepository(mySqlUrl, "username", "password", initializeConnPool);

        assertEquals(MySQLRepository.class, mySqlRepo.getClass());

        RelationalDBRepository oracleRepo = factory.createRDBRepository(oracleUrl, "username", "password", initializeConnPool);
        assertEquals(OracleRepository.class, oracleRepo.getClass());

        RelationalDBRepository nonExistingRepo = factory.createRDBRepository(postreSQLUrl, "username", "password", initializeConnPool);
        assertNull(nonExistingRepo);
    }

}