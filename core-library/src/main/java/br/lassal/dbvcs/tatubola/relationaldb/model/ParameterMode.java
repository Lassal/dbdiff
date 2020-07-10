package br.lassal.dbvcs.tatubola.relationaldb.model;

public enum ParameterMode {

    IN,
    OUT,
    INOUT;

    public static ParameterMode fromOracle(String parameterMode){
        if(parameterMode != null){
            switch (parameterMode){
                case "IN": return ParameterMode.IN;
                case "OUT": return ParameterMode.OUT;
                case "IN/OUT": return ParameterMode.INOUT;
                default: return null;
            }
        }

        return null;
    }

}
