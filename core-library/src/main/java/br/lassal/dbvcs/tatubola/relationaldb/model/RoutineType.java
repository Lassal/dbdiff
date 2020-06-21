package br.lassal.dbvcs.tatubola.relationaldb.model;

public enum RoutineType {
    FUNCTION,
    PROCEDURE,
    PACKAGE;

    public static RoutineType fromMySQL(String routineType){

        switch (routineType){
            case "FUNCTION" : return RoutineType.FUNCTION;
            case "PROCEDURE": return RoutineType.PROCEDURE;
            default: return null;
        }
    }

    public static RoutineType fromOracle(String routineType){

        switch (routineType){
            case "FUNCTION" : return RoutineType.FUNCTION;
            case "PROCEDURE": return RoutineType.PROCEDURE;
            case "PACKAGE" :  return RoutineType.PACKAGE;
            default: return null;
        }
    }
}
