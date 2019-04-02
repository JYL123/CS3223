package qp.operators;

import java.util.HashMap;
import java.util.Vector;
import qp.utils.*;

public class Distinct extends Operator {

    Batch inPage;
    Batch outPage;
    Operator op;
    Vector attr;
    int numTuplesPerPage;
    int bufferNum;
    HashMap<Integer, Vector> map;



    public Distinct(Operator op) {
        super(OpType.DISTINCT);
        this.op = op;

    }

    /** open the operator **/
    public boolean open() {
        numTuplesPerPage = Batch.getPageSize() / schema.getTupleSize();
        return true;
    }

    /** return the next page **/
    public Batch next() {
        System.out.println("DISTINCT: --------------");
        inPage = op.next();
        if (inPage == null) {
            return null;
        }
        return inPage;
    }

    /** close the operator **/
    public boolean close() {
        return true;
    }
}
