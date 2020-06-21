package br.lassal.dbvcs.tatubola.relationaldb.model;

public class ParentChildNode {

    private ParentChildNode parent;
    private String id;

    public ParentChildNode(String id){
        this.id = id;
    }


    public ParentChildNode getParent() {
        return parent;
    }

    public void setParent(ParentChildNode parent) {
        this.parent = parent;
    }


    public String getId() {
        return id;
    }

    public int getLevel(){
        if(parent != null){
            return this.parent.getLevel() + 1;
        }
        else{
            return 0;
        }
    }
}
