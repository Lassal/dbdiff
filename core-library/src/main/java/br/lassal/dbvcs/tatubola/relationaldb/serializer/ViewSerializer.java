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
        super(repository, dbModelFS, targetSchema, environmentName, ViewSerializer.logger);

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
        this.addLoadStep(this.getLoadViewDefinitionsStep(), "Load ViewDefinitions");
        this.addLoadStep(this.getLoadViewsReferencedTablesStep(), "Load Views ReferencedTables");
        this.addLoadStep(this.getLoadViewsColumnsStep(), "Load ViewColumns");
    }

    private LoadCommand getLoadViewDefinitionsStep() {
        ViewSerializer serializer = this;

        return () -> serializer.views = serializer.getRepository().loadViewDefinitions(serializer.getSchema());
    }

    private LoadCommand getLoadViewsReferencedTablesStep() {
        ViewSerializer serializer = this;

        return () -> serializer.referencedTables = serializer.getRepository().loadViewTables(serializer.getSchema());
    }

    private LoadCommand getLoadViewsColumnsStep() {
        ViewSerializer serializer = this;

        return () -> serializer.viewsColumns = serializer.getRepository().loadViewColumns(serializer.getSchema());
    }


}
