/** Block represents a number of pages **/

package qp.utils;

import java.io.Serializable;
import java.util.Vector;

public class Block implements Serializable {
    int MAX_SIZE;
    int pageSize;
    Vector<Batch> batches;
    Vector<Tuple> tuples;

    public Block(int numPage, int pageSize) {
        MAX_SIZE = numPage;
        this.pageSize = pageSize;
        batches = new Vector<>(MAX_SIZE);
        tuples = new Vector<>(pageSize * MAX_SIZE);
    }

    public Vector<Tuple> getTuples() {
        return tuples;
    }

    public Tuple getSingleTuple(int id) {
        return (Tuple) tuples.elementAt(id);
    }

    public void setTuples(Vector tupleList) {
        Batch b = new Batch(pageSize);
        for (int i = 0; i < tupleList.size(); i++) {
            if(b.isFull()) {
                batches.add(b);
                b = new Batch(pageSize);
            }
            b.add((Tuple) tupleList.get(i));
            tuples.add((Tuple) tupleList.get(i));
        }
        if (!b.isEmpty()) {
            batches.add(b);
        }
    }

    public Vector<Batch> getBatches() {
        return batches;
    }

    public Batch getSingleBatch(int id) {
        return (Batch) batches.elementAt(id);
    }

    public void setBatches(Vector<Batch> batches) {
        this.batches = batches;
        for (int i = 0; i < batches.size(); i++) {
            for (int j = 0; j < batches.get(i).size(); j++) {
                tuples.add(batches.get(i).elementAt(j));
            }
        }
    }

    public void addBatch(Batch b) {
        if (!isFull()) {
            batches.add(b);
            for (int i = 0; i < b.size(); i++) {
                tuples.add(b.elementAt(i));
            }
        }
    }

    public int getBatchSize() {
        return batches.size();
    }

    public int getTupleSize() {
        return tuples.size();
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }

    public boolean isFull() {
        if (batches.size() >= MAX_SIZE) {
            return true;
        }
        else {
            return false;
        }
    }
}
