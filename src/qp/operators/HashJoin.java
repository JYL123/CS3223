/**
 * Hash join algorithm
 **/

package qp.operators;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Objects;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class HashJoin extends Join {

    /**
     * The following fields are useful during execution of
     * * the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    Batch inputBuffer;
    Batch outputBuffer;
    Batch[] hashTable;

//    String rfname;    // The file name where the right table is materialize

    static int filenum = -1;   // To get unique filenum for this operation
    int currentFileNum;
    int[] partitionLeftPageCounts, partitionsRightPageCounts;

    int partitionCurs; // Cursor for the next partition
    int rPageCurs; // Cursor for next right page
    int rTupleCurs; // Cursor for next tuple in the right page to be probed

//    ObjectInputStream in; // File pointer to the right hand materialized file
//
//    int lcurs;    // Cursor for left side buffer
//    int rcurs;    // Cursor for right side buffer
//    boolean eosl;  // Whether end of stream (left table) is reached
//    boolean eosr;  // End of stream (right table)

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    public boolean open() {
        currentFileNum = ++filenum;
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        Batch[] partitions = new Batch[numBuff - 1];
        partitionLeftPageCounts = new int[numBuff - 1];
        partitionsRightPageCounts = new int[numBuff - 1];

        // Partition for left
        if (!partition(partitions, partitionLeftPageCounts, left, leftindex)) {
            return false;
        }

        // Partition for right
        if (!partition(partitions, partitionsRightPageCounts, right, rightindex)) {
            return false;
        }

        // Destroy partition buffers
        partitions = null;

        // Setup Buffer for hash table
        inputBuffer = new Batch(Batch.getPageSize() / left.schema.getTupleSize());
        outputBuffer = new Batch(Batch.getPageSize() / schema.getTupleSize());
        hashTable = new Batch[numBuff - 2];
        for (int i = 0; i < numBuff - 2; i++) {
            hashTable[i] = new Batch(Batch.getPageSize() / left.schema.getTupleSize());
        }

        // Setup first hash table (look for non-empty partition)
        for (int i = 0; i < partitionLeftPageCounts.length; i++) {
            if (populateHashTable(i)) {
                partitionCurs = i + 1;
                break;
            }
        }
        rPageCurs = 0;
        rTupleCurs = 0;

        return true;
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        // Probing Phase
        inputBuffer = new Batch(Batch.getPageSize() / right.schema.getTupleSize());

        int pageCount = partitionsRightPageCounts[partitionCurs - 1];
        // Read all pages of the partition from right
        for (int p = 0; p < pageCount; p++) {
            // For each page of a particular right partition
            // Read the page
            String fileName = generateFileName(partitionCurs - 1, p, false);
            if (!pageRead(inputBuffer, fileName)) {
                // Error reading partition
                System.exit(1);
            }

            // For each tuple
            for (int t = 0; t < inputBuffer.size(); t++) {
                Tuple tuple = inputBuffer.elementAt(t);
                int hash = hashFn2(tuple, rightindex, hashTable.length);
                // Find match in hash table
//                hashTable[hash]
            }

        }
    }


    /**
     * Close the operator
     */
    public boolean close() {
        return true;
    }

    private int hashFn1(Tuple t, int index, int bucketSize) {
        final int PRIME = 769;
        Object value = t.dataAt(index);
        return (Objects.hash(value) * PRIME) % bucketSize;
    }

    private int hashFn2(Tuple t, int index, int bucketSize) {
        final int PRIME = 12289;
        Object value = t.dataAt(index);
        return (Objects.hash(value) * PRIME) % bucketSize;
    }

    private boolean pageWrite(Batch b, String fileName) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));
            out.writeObject(b);
            out.close();
        } catch (IOException io) {
            System.out.println("Hash Join : writing the temporary file error === " + fileName);
            return false;
        }
        return true;
    }

    private boolean pageRead(Batch b, String fileName) {
        try {
            ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileName));
        } catch (IOException io) {
            System.err.println("Hash Join : error in reading the file === " + fileName);
            return false;
        }
        return true;
    }

    private String generateFileName(int partitionNum, int pageNum, boolean isLeft) {
        if (isLeft) {
            return String.format("HJtemp%d-LeftPartition%d-%d", currentFileNum, partitionNum,
                    pageNum);
        } else {
            return String.format("HJtemp%d-RightPartition%d-%d", currentFileNum, partitionNum,
                    pageNum);
        }

    }

    private boolean partition(Batch[] partitions, int[] partitionsCount, Operator op,
                              int index) {
        boolean isLeft = (op == left);
        int tupleCount = Batch.getPageSize() / op.schema.getTupleSize();
        for (int i = 0; i < numBuff - 1; i++) {
            partitionsCount[i] = 0;
            partitions[i] = new Batch(tupleCount);
        }

        if (!op.open()) {
            return false;
        }

        while ((inputBuffer = op.next()) != null) {
            for (int i = 0; i < inputBuffer.size(); i++) {
                Tuple t = inputBuffer.elementAt(i);
                int bucket = hashFn1(t, index, partitions.length);
                Batch partition = partitions[bucket];
                if (partition.isFull()) {
                    String fileName = generateFileName(bucket, partitionsCount[bucket], isLeft);
                    pageWrite(partition, fileName);
                    partitionsCount[bucket]++;
                }
                partition.insertElementAt(t, partition.size());
            }
        }

        // Write out all non-full partitions
        for (int i = 0; i < partitions.length; i++) {
            Batch partition = partitions[i];
            if (partition.isEmpty()) {
                break;
            }
            String fileName = generateFileName(i, partitionsCount[i], isLeft);
            pageWrite(partition, fileName);
            partitionsCount[i]++;
        }
        return true;
    }

    private boolean populateHashTable(int partitionNum) {
        int pageCount = partitionLeftPageCounts[partitionNum];
        if (pageCount < 1) {
            return false;
        }

        // Read all pages of the partition from left
        for (int p = 0; p < pageCount; p++) {
            // Read a page of a partition
            String fileName = generateFileName(partitionNum, p, true);
            if (!pageRead(inputBuffer, fileName)) {
                // Error reading partition
                System.out.println("Hash Join : reading the file error === " + fileName);
                System.exit(1);
            }

            // Populate hash table
            for (int t = 0; t < inputBuffer.size(); t++) {
                Tuple tuple = inputBuffer.elementAt(t);
                int hash = hashFn2(tuple, leftindex, hashTable.length);
                if (hashTable[hash].isFull()) {
                    System.out.printf("Hash table (bucket: %d) is too small\n", hash);
                }
                hashTable[hash].insertElementAt(tuple, hashTable[hash].size());
            }
        }
        return true;
    }

}







































