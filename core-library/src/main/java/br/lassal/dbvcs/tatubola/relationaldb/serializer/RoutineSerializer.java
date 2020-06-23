package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Routine;
import br.lassal.dbvcs.tatubola.relationaldb.model.RoutineParameter;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RoutineSerializer extends DBModelSerializer<Routine>{

    private List<Routine> routines;
    private List<RoutineParameter> routinesParameters;

    public RoutineSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema){
        super(repository, dbModelFS, targetSchema );
    }


    List<Routine> assemble(){

        Map<String, Routine> routinesMap = this.routines.stream().collect(Collectors.toMap(Routine::getRoutineID, Function.identity()));

        Routine currentRoutine = null;
        for(RoutineParameter param : this.routinesParameters){

            if(currentRoutine == null || !param.getRoutineID().equals(currentRoutine.getRoutineID())){
                currentRoutine = routinesMap.containsKey(param.getRoutineID()) ?
                        routinesMap.get(param.getRoutineID()) : null;
            }

            if(currentRoutine != null){
                if(param.getOrdinalPosition() == 0){
                    currentRoutine.setReturnParamater(param);
                }
                else{
                    currentRoutine.addParameter(param);
                }
            }
        }

        return this.routines;

    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadRoutineDefinitionStep());
        this.addLoadStep(this.getLoadRoutinesParameters());
    }

    private LoadCommand getLoadRoutineDefinitionStep(){
        RoutineSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                 serializer.routines = serializer.getRepository().loadRoutineDefinition(serializer.getSchema());
            }
        };
    }

    private LoadCommand getLoadRoutinesParameters(){
        RoutineSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.routinesParameters = serializer.getRepository().loadRoutineParameters(serializer.getSchema());
            }
        };
    }

}
