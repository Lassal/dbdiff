package br.lassal.dbvcs.tatubola.fs;

public class DBModelFSException extends Exception {

    public DBModelFSException() {
        super("Object path NULL. Have you defined the path for this type of DB object ?");
    }
}
