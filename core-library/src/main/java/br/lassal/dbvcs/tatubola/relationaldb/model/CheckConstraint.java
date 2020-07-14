package br.lassal.dbvcs.tatubola.relationaldb.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonPropertyOrder({"name", "type", "checkClause"})
public class CheckConstraint extends br.lassal.dbvcs.tatubola.relationaldb.model.TableConstraint {

    private String checkClause;

    public CheckConstraint() {
    }

    public CheckConstraint(String constraintSchema, String tableName, String constraintName, String checkClause) {
        super(constraintSchema, tableName, constraintName, ConstraintType.CHECK);
        this.checkClause = checkClause;
    }


    public String getCheckClause() {
        return checkClause;
    }

    public void setCheckClause(String checkClause) {
        this.checkClause = checkClause;
    }


    @Override
    public void onAfterLoad() {
        // abstract method inherited from TableConstraint
        // no action is required after load in this class
    }

    @Override
    public String toString() {
        return super.toString() + "Check clause: " + this.checkClause;
    }

    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;

        if (obj instanceof CheckConstraint) {
            CheckConstraint other = (CheckConstraint) obj;

            isEqual = super.equals(other);
            isEqual &= this.checkClause.equals(other.checkClause);
        }

        return isEqual;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getTableID(), this.getName(), this.getType(), this.checkClause);
    }
}
