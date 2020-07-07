package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import br.lassal.dbvcs.tatubola.relationaldb.model.TableConstraint;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSerializer extends DBModelSerializer<Table>{

    private static Logger logger = LoggerFactory.getLogger(TableSerializer.class);

    private Map<String, Table> tables;
    private List<TableConstraint> tableConstraints;

    public TableSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName){
        super(repository, dbModelFS, targetSchema, environmentName );

        this.setLogger(logger);
    }

    List<Table> assemble(){

        for (TableConstraint constraint: this.tableConstraints) {
            if(this.tables.containsKey(constraint.getTableID())){
                this.tables.get(constraint.getTableID()).addConstraint(constraint);
            }
        }

        this.tables.values().stream().forEach(Table::onAfterLoad);

        return this.tables.values().stream()
                .sorted(Comparator.comparing(Table::getTableID))
                .collect(Collectors.toList());

    }

    @Override
    protected void defineLoadSteps() {
         this.addLoadStep(this.getLoadTableColumnsStep());
         this.addLoadStep(this.getLoadTableConstraintsStep());
    }

    private LoadCommand getLoadTableColumnsStep(){
        TableSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.trace("loadTableColumns", "before load");
                serializer.tables = serializer.getRepository().loadTableColumns(serializer.getSchema());
                serializer.trace("loadTableColumns", "after load");
            }
        };
    }

    private LoadCommand getLoadTableConstraintsStep(){
        TableSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                String schema = serializer.getSchema();

                serializer.trace("load table constraints", "before load");

                serializer.tableConstraints = serializer.getRepository().loadCheckConstraints(schema);

                List<TableConstraint> uniqueConstraints = serializer.getRepository().loadUniqueConstraints(schema);
                if(uniqueConstraints != null){
                    serializer.tableConstraints.addAll(uniqueConstraints);
                }

                List<TableConstraint> refConstraints = serializer.getRepository().loadReferentialConstraints(schema);
                if(refConstraints != null){
                    serializer.tableConstraints.addAll(refConstraints);
                }

                serializer.trace("load table constraints", "after load");
            }
        };
    }

}
