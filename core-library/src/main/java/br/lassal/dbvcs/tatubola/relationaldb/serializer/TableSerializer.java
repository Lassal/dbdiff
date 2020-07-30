package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import br.lassal.dbvcs.tatubola.relationaldb.model.TableConstraint;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableSerializer extends DBModelSerializer<Table> {

    private static Logger logger = LoggerFactory.getLogger(TableSerializer.class);

    private Map<String, Table> tables;
    private List<TableConstraint> tableConstraints;

    public TableSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName) {
        super(repository, dbModelFS, targetSchema, environmentName, logger);
    }

    List<Table> assemble() {

        for (TableConstraint constraint : this.tableConstraints) {
            if (this.tables.containsKey(constraint.getTableID())) {
                this.tables.get(constraint.getTableID()).addConstraint(constraint);
            }
        }

        return this.tables.values().stream()
                .collect(Collectors.toList());

    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadTableColumnsStep(), "Load TableColumns");
        this.addLoadStep(this.getLoadTableConstraintsStep(), "Load TableConstraints");
    }

    private LoadCommand getLoadTableColumnsStep() {
        TableSerializer serializer = this;

        return () -> serializer.tables = serializer.getRepository().loadTableColumns(serializer.getSchema());

    }

    private LoadCommand getLoadTableConstraintsStep() {
        TableSerializer serializer = this;

        return () -> {

            String schema = serializer.getSchema();

            serializer.tableConstraints = serializer.getRepository().loadCheckConstraints(schema);

            List<TableConstraint> uniqueConstraints = serializer.getRepository().loadUniqueConstraints(schema);
            if (uniqueConstraints != null) {
                serializer.tableConstraints.addAll(uniqueConstraints);
            }

            List<TableConstraint> refConstraints = serializer.getRepository().loadReferentialConstraints(schema);
            if (refConstraints != null) {
                serializer.tableConstraints.addAll(refConstraints);
            }
        };
    }

}
