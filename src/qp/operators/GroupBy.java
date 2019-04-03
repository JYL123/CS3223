/**
 * To project out the group by attributes from the result
 **/

package qp.operators;

import qp.utils.*;

import java.util.HashSet;
import java.util.Objects;
import java.util.Vector;

public class GroupBy extends Operator {

    Operator base;
    Vector attrSet;
    int batchsize;  // number of tuples per outbatch

    Batch inbatch;
    Batch outbatch;
    HashSet<Vector> hashSet;

    int inputCurs;
    /**
     * index of the attributes in the base operator
     * * that are to be group by
     **/
    int[] attrIndex;


    public GroupBy(Operator base, Vector as, int type) {
        super(type);
        this.base = base;
        this.attrSet = as;

    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public Vector getGroupAttr() {
        return attrSet;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/

    public boolean open() {
        /** setnumber of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        hashSet = new HashSet<>();
        inputCurs = -1;

        /** The followingl loop findouts the index of the columns that
         ** are required from the base operator
         **/

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];
        //System.out.println("Project---Schema: ----------in open-----------");
        //System.out.println("base Schema---------------");
        //Debug.PPrint(baseSchema);
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;

            //  Debug.PPrint(attr);
            //System.out.println("  "+index+"  ");
        }

        if (base.open())
            return true;
        else
            return false;
    }

    /**
     * Read next tuple from operator
     */

    public Batch next() {
        //System.out.println("Project:-----------------in next-----------------");
        outbatch = new Batch(batchsize);

        // all the tuples in the inbuffer goes to the output buffer
        inbatch = base.next();
        // System.out.println("Project:-------------- inside the next---------------");

        if (inbatch == null) {
            return null;
        }

        //System.out.println("Project:---------------base tuples---------");
        for (int i = inputCurs + 1; i < inbatch.size(); i++) {
            inputCurs = i;
            Tuple basetuple = inbatch.elementAt(i);
            Vector present = getGroupByValues(basetuple);
            if (!hashSet.contains(present)) {
                hashSet.add(present);
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);
                if (outbatch.isFull()) {
                    return outbatch;
                }
            }
        }
        inputCurs = -1;

        return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        return true;
		/*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for (int i = 0; i < attrSet.size(); i++)
            newattr.add((Attribute) ((Attribute) attrSet.elementAt(i)).clone());
        GroupBy newproj = new GroupBy(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

    private Vector getGroupByValues(Tuple t) {
        Vector values = new Vector();
        for (int i : this.attrIndex) {
            values.add(t.dataAt(i));
        }
        return values;
    }
}
