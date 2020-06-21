package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import br.lassal.dbvcs.tatubola.relationaldb.model.TableConstraint;
import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSerializer extends DBModelSerializer<Table>{

    private Map<String, Table> tables;
    private List<TableConstraint> tableConstraints;

    public TableSerializer(MySQLRepository repository, String targetSchema, String outputPath){
        super(repository, targetSchema, outputPath);
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
                serializer.tables = serializer.getRepository().loadTableColumns(serializer.getSchema());
            }
        };
    }

    private LoadCommand getLoadTableConstraintsStep(){
        TableSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.tableConstraints = serializer.getRepository().loadCheckConstraints(serializer.getSchema());
                serializer.tableConstraints.addAll( serializer.getRepository().loadPKFKUniqueConstraints(serializer.getSchema()));
            }
        };
    }

}
