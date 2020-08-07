package br.lassal.dbvcs.tatubola.relationaldb.model;

import br.lassal.dbvcs.tatubola.relationaldb.repository.OracleRepository;

public class RoutineDummyBuilder {

    public Routine createProcedure(String schema, String routineName, int id) {
        Routine routine = new Routine(schema, routineName + "_" + id, RoutineType.PROCEDURE);
        routine.addParameter(this.createRoutineParameter(schema, routine.getName(),"DummyParam01", 1, "VARCHAR2", ParameterMode.IN));
        routine.addParameter(this.createRoutineParameter(schema, routine.getName(),"DummyParam02", 2, "NUMBER", ParameterMode.IN));
        routine.addParameter(this.createRoutineParameter(schema, routine.getName(),"DummyParam03", 3, "NUMBER", ParameterMode.OUT));

        routine.setRoutineDefinition("BEGIN   /* declare local variables */   DECLARE a INT DEFAULT 10;   DECLARE b, c INT;    /* using the local variables */   SET a = a + 100;   SET b = 2;   SET c = a + b;    BEGIN      /* local variable in nested block */      DECLARE c INT;             SET c = 5;       /* local variable c takes precedence over the one of the          same name declared in the enclosing block. */       SELECT a, b, c;   END;    SELECT a, b, c;");

        return routine;
    }

    public Routine createFunction(String schema, String routineName, int id) {
        Routine routine = new Routine(schema, routineName + "_" + id, RoutineType.FUNCTION);
        routine.setReturnParamater(new TypedColumn(RoutineParameter.RETURN_PARAMETER_NAME, 0, "VARCHAR2"));
        routine.addParameter(this.createRoutineParameter(schema, routine.getName(),"Param01", 1, "NUMBER", ParameterMode.IN));
        routine.addParameter(this.createRoutineParameter(schema, routine.getName(),"Param02", 2, "DATETIME", ParameterMode.IN));

        routine.setRoutineDefinition("CREATE OR REPLACE FUNCTION totalRecords ()RETURNS integer AS $total$declaretotal integer;BEGIN   SELECT count(*) into total FROM COMPANY;   RETURN total;END;$total$ LANGUAGE plpgsql;");


        return routine;
    }

    private RoutineParameter createRoutineParameter(String schema, String routineName, String paramName, int ordinalPosition
            , String dataType ,ParameterMode parameterMode){
        RoutineParameter parameter = new RoutineParameter(paramName, ordinalPosition, dataType, parameterMode );
        parameter.setRoutineSchema(schema);
        parameter.setRoutineName(routineName);

        return parameter;
    }

}
