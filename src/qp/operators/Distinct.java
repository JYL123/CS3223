package qp.operators;

import java.util.HashMap;
import java.util.Vector;

import qp.utils.*;

public class Distinct extends Operator {

    Batch inPage;
    boolean eos;

    Operator base;
    Vector attrSet;
    int batchsize;
//    int numBuffer;

    int[] attrIndex;
    HashMap<Integer, Vector> hm = new HashMap<Integer, Vector>();



    public Distinct(Operator base, int type, Vector attrSet) {
        super(type);
        this.base = base;
        this.attrSet = attrSet;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public Vector getProjAttr() {
        return attrSet;
    }

    /** open the connection **/
    public boolean open() {
        eos = false;
        /** num of tuples per batch**/
        int tuplesize = schema.getTupleSize();
        batchsize= Batch.getPageSize()/tuplesize;

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];

        System.out.println("base Schema---------------");
        Debug.PPrint(baseSchema);
        System.out.println("\nbase: --------------------");
        Debug.PPrint(base);
        System.out.println("\n$$$$$$$$$$$$$$$$$$  " + base.getClass() + "  $$$$$$$$$$$$$");

        for (int i = 0; i < attrSet.size(); i++) {
            Attribute a = (Attribute) attrSet.elementAt(i);
            System.out.println("\nDISTINCT attribute: -------------------------------");
            Debug.PPrint(a);
            // get the index of columns of the table,
            // and add it to the array which tracks these index
            int id = baseSchema.indexOf(a);
            attrIndex[i] = id;
        }

        if (base.open()) {
            return true;
        }
        else {
            return false;
        }
    }

    /** return the output buffer once it is full **/
    public Batch next() {
        System.out.print("\nDISTINCT:--------------------------in next----------------");

        if (eos) {
            close();
            return null;
        }

        // initialise an output buffer,
        // a vector storing the last vector read in the inPage
        // and a cursor which positions at the input buffer
        Batch outPage = new Batch(batchsize);
        Vector last = null;
        int pos = 0;



        while (!outPage.isFull()) {
            if (pos == 0) { /** reading a new incoming page is required **/
//                System.out.println("DISTINCT: read next -------------- ");

                inPage = base.next();
                // int size = inPage.size();
                // System.out.println("================================================== page size = " + size);

                /** check if it reaches the end of incoming pages **/
                if (inPage == null || inPage.size() == 0) {
                    eos = true;
                    return outPage;
                }

            }
//            System.out.println("DISTINCT: check incoming page ----------------");

            for (int i = pos; i < inPage.size(); i++) {
                System.out.println("debugging message +++++++++++++++++++++++[" + i + "]");

                Tuple tuple = inPage.elementAt(i);

                Debug.PPrint(tuple);

                Vector current = new Vector();
                for (int j = 0; j < attrSet.size(); j++) {
                    Object data = tuple.dataAt(attrIndex[j]);
                    current.add(data);
                }
                /** check for duplicate entry **/
                System.out.println("~~~~~~~~~~~ " + current.getClass() + " ~~~~~~~~~~~~~");
                if (hm.containsValue(current)) {
                    continue;
                }
                else {
                    hm.put(i, current);
                    outPage.add(new Tuple(current));
                }
//                if (current.equals(last)) {
//                    continue;
//                }
                /** add entry to the output buffer **/
//                last = current;

                if (outPage.isFull()) {
//                    if (i == inPage.size()) {
//                        pos = 0;
//                    }
//                    else {
//                        pos = i + 1;
//                    }
                    return outPage;
                }
            }
            pos = 0;
            int sizeo = outPage.size();
            System.out.println("$$$$$$$$$$$ size of outpage = " + sizeo);
        }
        return outPage;
    }

    /** close the operator **/
    public boolean close() {
        return true;
    }

    public Object clone() {
        Operator newBase = (Operator) base.clone();
        Vector newAttrSet = new Vector();
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute a = (Attribute) ((Attribute) attrSet.elementAt(i)).clone();
            newAttrSet.add(a);
            System.out.println("\nDebugging for Attribute a: ----------------------");
            Debug.PPrint(a);
        }
        Distinct newProj = new Distinct(newBase, optype, newAttrSet);
        Schema newSchema = newBase.getSchema().subSchema(newAttrSet);
        newProj.setSchema(newSchema);
        return newProj;


    }

}
