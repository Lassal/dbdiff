package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.fs.DBModelFS;
import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import br.lassal.dbvcs.tatubola.relationaldb.model.TableColumn;
import br.lassal.dbvcs.tatubola.relationaldb.model.View;
import br.lassal.dbvcs.tatubola.relationaldb.repository.RelationalDBRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ViewSerializer extends DBModelSerializer<View> {

    private static Logger logger = LoggerFactory.getLogger(ViewSerializer.class);

    private List<View> views;
    private Map<String, List<Table>> referencedTables;
    private Map<String, List<TableColumn>> viewsColumns;

    public ViewSerializer(RelationalDBRepository repository, DBModelFS dbModelFS, String targetSchema, String environmentName) {
        super(repository, dbModelFS, targetSchema, environmentName );

        this.setLogger(logger);

    }

    @Override
    List<View> assemble() {
        Map<String, View> mapViews = this.views.stream().collect(Collectors.toMap(View::getViewID, Function.identity()));

        for (Map.Entry<String, View> view : mapViews.entrySet()) {

            if (this.referencedTables.containsKey(view.getKey())) {
                List<Table> viewTables = this.referencedTables.get(view.getKey());

                if (viewTables.isEmpty()) {
                    view.getValue().setReferencedTables(viewTables);
                }
            }

            if (this.viewsColumns.containsKey(view.getKey())) {
                List<TableColumn> columns = this.viewsColumns.get(view.getKey());

                if (columns.isEmpty()) {
                    view.getValue().setColumns(columns);
                }
            }
        }

        return this.views;
    }

    @Override
    protected void defineLoadSteps() {
        this.addLoadStep(this.getLoadViewDefinitionsStep());
        this.addLoadStep(this.getLoadViewsReferencedTablesStep());
        this.addLoadStep(this.getLoadViewsColumnsStep());
    }

    private LoadCommand getLoadViewDefinitionsStep() {
        ViewSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.trace("loadViewDefinitions", "before load");
                serializer.views = serializer.getRepository().loadViewDefinitions(serializer.getSchema());
                serializer.trace("loadViewDefinitions", "after load");
            }
        };
    }

    private LoadCommand getLoadViewsReferencedTablesStep() {
        ViewSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.trace("loadViewTables", "before load");
                serializer.referencedTables = serializer.getRepository().loadViewTables(serializer.getSchema());
                serializer.trace("loadViewTables", "after load");
            }
        };
    }

    private LoadCommand getLoadViewsColumnsStep() {
        ViewSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.trace("loadViewColumns", "before load");
                serializer.viewsColumns = serializer.getRepository().loadViewColumns(serializer.getSchema());
                serializer.trace("loadViewColumns", "after load");
            }
        };
    }


}
