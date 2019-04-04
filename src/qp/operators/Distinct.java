package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.Vector;

import static qp.utils.Tuple.compareTuples;

public class Distinct extends SortMerge {

    Batch inPage;
    Batch outPage;

    Tuple last;
    int pos; // a cursor which positions at the input buffer

    boolean eos;

    public Distinct(Operator base, Vector attrSet, int opType) {
        super(base, attrSet, opType);
    }

    @Override
    public boolean open() {
        eos = false;
        pos = 0;
        last = null;
        return super.open();
    }

    @Override
    public Batch next() {
        int i;
        if (eos) {
            super.close();
            return null;
        }

        outPage = new Batch(batchsize);

        while (!outPage.isFull()) {
            if (pos == 0) { /** reading a new incoming page is required **/
//                System.out.println("DISTINCT: read next -------------- ");
                inPage = super.next();
                // int size = inPage.size();
                // System.out.println("================================================== page size = " + size);

                /** check if it reaches the end of incoming pages **/
                if (inPage == null || inPage.size() == 0) {
                    eos = true;
                    return outPage;
                }
            }

            //            System.out.println("DISTINCT: check incoming page ----------------");

            for (i = pos; i < inPage.size(); i++) {
                System.out.println("debugging message +++++++++++++++++++++++[" + i + "]");

                Tuple current = inPage.elementAt(i);

//                Debug.PPrint(tuple);


                int result = 0;
                for (int index: attrIndex) {
                    result = Tuple.compareTuples(last, current, index, index);
                    if (result != 0) {
                        break;
                    }
                }
                if (last == null || (result != 0)) {
                    outPage.add(current);
                    last = current;
                } else {
                    continue;
                }

                if (outPage.isFull()) {
                    return outPage;
                }
            }

            if (i == inPage.size()) {
                pos = 0;
            } else {
                pos = i;
            }
        }
        return outPage;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newAttr = (Vector) attrSet.clone();
        Distinct newDistinct = new Distinct(newbase, newAttr, optype);
        newDistinct.setSchema(newbase.getSchema());
        return newDistinct;
    }
}