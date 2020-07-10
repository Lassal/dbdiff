package br.lassal.dbvcs.tatubola.relationaldb.model;

public enum ConstraintType {
    PRIMARY_KEY(1),
    FOREIGN_KEY(2),
    UNIQUE(3),
    CHECK(4);

    private int order;

    ConstraintType(int order) {
        this.order = order;
    }

    public int getOrder() {
        return this.order;
    }

    // Helper methods

    public static ConstraintType fromMySQL(String mySQLConstraintType) {

        switch (mySQLConstraintType) {
            case "CHECK":
                return ConstraintType.CHECK;
            case "PRIMARY KEY":
                return ConstraintType.PRIMARY_KEY;
            case "FOREIGN KEY":
                return ConstraintType.FOREIGN_KEY;
            case "UNIQUE":
                return ConstraintType.UNIQUE;
            default:
                return null;
        }
    }

    public static ConstraintType fromOracle(String oracleConstraintType) {
        switch (oracleConstraintType) {
            case "C":
                return ConstraintType.CHECK;
            case "P":
                return ConstraintType.PRIMARY_KEY;
            case "R":
                return ConstraintType.FOREIGN_KEY;
            case "U":
                return ConstraintType.UNIQUE;
            default:
                return null;
        }
    }
}
