package br.lassal.dbvcs.tatubola.relationaldb.model;

public class ViewDummyBuilder {

    public View createView(String schema, String viewName, int id){
        View view = new View(schema, viewName);
        view.setUpdatedAllowed(true);
        view.setInsertAllowed(false);
        view.setViewDefinition("CREATE OR REPLACE FUNCTION totalRecords ()RETURNS integer AS $total$declaretotal integer;BEGIN   SELECT count(*) into total FROM COMPANY;   RETURN total;END;$total$ LANGUAGE plpgsql;");
        view.addTable(schema, "TableRef01_" + id);
        view.addTable(schema, "TableRef02");

        TableColumnDummyBuilder colBuilder = new TableColumnDummyBuilder();

        view.addColumn(colBuilder.createTextColumn(1, 50, false));
        view.addColumn(colBuilder.createTextColumn(2, 30, false));
        view.addColumn(colBuilder.createNumericColumn(3, 20, 6, false));
        view.addColumn(colBuilder.createNumericColumn(4, 15, 3, true));

        return view;
    }

}
