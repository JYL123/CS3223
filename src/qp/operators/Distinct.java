package qp.operators;

import java.util.HashMap;
import java.util.Vector;
import qp.utils.*;

public class Distinct extends Operator {

    Batch inPage;

    Operator base;
    Vector attrSet;
    int numTuplesPerPage;
//    int numBuffer;

    int[] attrIndex;



    public Distinct(Operator base) {
        super(OpType.DISTINCT);
        this.base = base;

    }

    /** open the operator **/
    public boolean open() {
        numTuplesPerPage = Batch.getPageSize() / schema.getTupleSize();

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute a = (Attribute) attrSet.elementAt(i);
            // get the index of columns of the table,
            // and add it to the array which tracks these index
            int id = baseSchema.indexOf(a);
            attrIndex[i] = id;
        }
        return true;
    }

    /** return the output buffer once it is full **/
    public Batch next() {

        // initialise an output buffer,
        // a vector storing the last vector read in the inPage
        // and a cursor which positions at the input buffer
        Batch outPage = new Batch(numTuplesPerPage);
        Vector last = null;
        int pos = 0;

        while (!outPage.isFull()) {
            if (pos == 0) { /** reading a new incoming page is required **/
                System.out.println("DISTINCT: read next --------------");
                inPage = base.next();
                /** check if it reaches the end of incoming pages **/
                if (inPage == null) {
                    return outPage;
                }
            }
            System.out.println("DISTINCT: check incoming page ----------------");
            for (int i = pos; i < inPage.size(); i++) {
                Tuple tuple = inPage.elementAt(i);

                System.out.println("debugging message");
                Tuple.printTuple(tuple);

                Vector current = new Vector();
                for (int j = 0; j < attrSet.size(); j++) {
                    Object data = tuple.dataAt(attrIndex[j]);
                    current.add(data);
                }
                /** check for duplicate entry **/
                if (current.equals(last)) {
                    continue;
                }
                /** add entry to the output buffer **/
                last = current;
                outPage.add(new Tuple(current));

                if (outPage.isFull()) {
                    return outPage;
                }
            }
            pos = 0;
        }
        return outPage;
    }

    /** close the operator **/
    public boolean close() {
        return true;
    }
}
