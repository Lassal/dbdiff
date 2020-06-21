package br.lassal.dbvcs.tatubola.relationaldb.serializer;

import br.lassal.dbvcs.tatubola.relationaldb.model.Table;
import br.lassal.dbvcs.tatubola.relationaldb.model.TableColumn;
import br.lassal.dbvcs.tatubola.relationaldb.model.View;
import br.lassal.dbvcs.tatubola.relationaldb.repository.MySQLRepository;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ViewSerializer extends DBModelSerializer<View> {

    private List<View> views;
    private Map<String, List<Table>> referencedTables;
    private Map<String, List<TableColumn>> viewsColumns;

    public ViewSerializer(MySQLRepository repository, String targetSchema, String outputPath) {
        super(repository, targetSchema, outputPath);

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
                serializer.views = serializer.getRepository().loadViewDefinitions(serializer.getSchema());
            }
        };
    }

    private LoadCommand getLoadViewsReferencedTablesStep() {
        ViewSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.referencedTables = serializer.getRepository().loadViewTables(serializer.getSchema());
            }
        };
    }

    private LoadCommand getLoadViewsColumnsStep() {
        ViewSerializer serializer = this;

        return new LoadCommand() {
            @Override
            public void execute() {
                serializer.viewsColumns = serializer.getRepository().loadViewColumns(serializer.getSchema());
            }
        };
    }


}
