package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Routine;
import br.lassal.dbvcs.tatubola.relationaldb.model.RoutineParameter;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RoutineSerializer extends DBModelSerializer<Routine> {

    private static Logger logger = LoggerFactory.getLogger(RoutineSerializer.class);

    private List<Routine> routines;
    private List<RoutineParameter> routinesParameters;

    public RoutineSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName) {
        super(repository, dbModelFS, targetSchema, environmentName, logger);

    }


    List<Routine> assemble() {

        if(this.routines != null){
            Map<String, Routine> routinesMap = this.routines.stream().collect(Collectors.toMap(Routine::getRoutineID, Function.identity()));

            if(this.routinesParameters != null){
                Routine currentRoutine = null;
                for (RoutineParameter param : this.routinesParameters) {

                    if (currentRoutine == null || !param.getRoutineID().equals(currentRoutine.getRoutineID())) {
                        currentRoutine = routinesMap.containsKey(param.getRoutineID()) ?
                                routinesMap.get(param.getRoutineID()) : null;
                    }

                    if (currentRoutine != null) {
                        if (param.getOrdinalPosition() == 0) {
                            currentRoutine.setReturnParamater(param);
                        } else {
                            currentRoutine.addParameter(param);
                        }
                    }
                }
            }

            return this.routines;

        }
        else{
            logger.warn(String.format("The repository return NULL for the routines in Environment: %s | Schema: %s",
                    this.getEnvironmentName(), this.getSchema()));

            return new ArrayList<>();
        }

    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadRoutineDefinitionStep(), "Load RoutineDefinitions");
        this.addLoadStep(this.getLoadRoutinesParameters(), "Load RoutineParametes");
    }

    private LoadCommand getLoadRoutineDefinitionStep() {
        RoutineSerializer serializer = this;

        return () -> serializer.routines = serializer.getRepository().loadRoutineDefinition(serializer.getSchema());
    }

    private LoadCommand getLoadRoutinesParameters() {
        RoutineSerializer serializer = this;

        return () -> serializer.routinesParameters = serializer.getRepository().loadRoutineParameters(serializer.getSchema());
    }

}
