/**
 * Hash join algorithm
 **/

package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Objects;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class HashJoin extends Join {

    /**
     * The following fields are useful during execution of
     * * the Hash Join operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    Batch inputBuffer;
    Batch outputBuffer;
    Batch[] hashTable;

    static int filenum = -1;   // To get unique filenum for this operation
    int currentFileNum;
    ArrayList<String> fileNames = new ArrayList<>();
    int[] partitionLeftPageCounts, partitionsRightPageCounts;

    int partitionCurs; // Current partition cursor
    int rPageCurs; // Current right page cursor
    int rTupleCurs; // Last probed tuple in the right page
    int lTupleCurs; // Last checked tuple in a particular bucket of the hash table

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
        hashTable = new Batch[numBuff - 2];
        for (int i = 0; i < numBuff - 2; i++) {
            hashTable[i] = new Batch(Batch.getPageSize() / left.schema.getTupleSize());
        }

        // Build first hash table (from a non-empty partition)
        partitionCurs = -1;
        for (int i = 0; i < partitionLeftPageCounts.length; i++) {
            if (buildHashTable(i)) {
                partitionCurs = i;
                break;
            }
        }
        rPageCurs = 0;
        rTupleCurs = -1;
        lTupleCurs = -1;
        // Prepare buffer for reading of right and output of joined tuples
        inputBuffer = new Batch(Batch.getPageSize() / right.schema.getTupleSize());
        outputBuffer = new Batch(Batch.getPageSize() / schema.getTupleSize());

        return true;
    }


    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/


    public Batch next() {
        // Prepare (clean) output buffer for new set of joined tuples
        outputBuffer.clear();

        for (int parti = partitionCurs; parti < partitionLeftPageCounts.length; parti++) {
            // BUILDING PHASE
            if (parti != partitionCurs) {
                // Probing partition[parti], but hash table (is at partitionCurs) not updated
                if (buildHashTable(parti)) {
                    partitionCurs = parti;
                } else {
                    // Error reading partition or no tuple in the left of this partition
                    // Move to next partition to build hash table
                    continue;
                }
            }

            // PROBING PHASE
            // Read all pages of the partition from right
            for (int p = rPageCurs; p < partitionsRightPageCounts[partitionCurs]; p++) {
                // For each page of a particular right partition
                rPageCurs = p;
                // Read the page
                String fileName = generateFileName(partitionCurs, p, false);
                if (!pageRead(fileName)) {
                    // Error reading partition
                    System.exit(1);
                }

                // For each tuple
                for (int t = rTupleCurs + 1; t < inputBuffer.size(); t++) {
                    rTupleCurs = t;
                    Tuple tuple = inputBuffer.elementAt(t);
                    // Find match(s) in hash table
                    Tuple foundTupleLeft;
                    while ((foundTupleLeft = findMatchInHashtable(hashTable, leftindex, tuple, rightindex)) != null) {
                        outputBuffer.add(foundTupleLeft.joinWith(tuple));
                        // Return output buffer, if full
                        if (outputBuffer.isFull()) {
                            // rPageCurs = p;
                            rTupleCurs = t - 1; // Come back to this tuple as there might be other matches in the
                            // same bucket
                            return outputBuffer;
                        }
                    }
                }
                rTupleCurs = -1;
            }
            rPageCurs = 0;
            rTupleCurs = -1;
        }
        // End of probing
        if (outputBuffer.isEmpty()) {
            close();
            return null;
        }
        return outputBuffer;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        // Delete all partition files
        for (int i = 0; i < fileNames.size(); i++) {
            File f = new File(fileNames.get(i));
            f.delete();
        }

        inputBuffer.clear();
        inputBuffer = null;
        outputBuffer.clear();
        outputBuffer = null;
        for (int i = 0; i < numBuff - 2; i++) {
            hashTable[i].clear();
        }
        hashTable = null;

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
            fileNames.add(fileName);
            out.close();
        } catch (IOException io) {
            System.out.println("Hash Join : writing the temporary file error === " + fileName);
            return false;
        }
        return true;
    }

    private boolean pageRead(String fileName) {
        ObjectInputStream in;
        try {
            in = new ObjectInputStream(new FileInputStream(fileName));
        } catch (IOException io) {
            System.err.println("Hash Join : error in reading the file === " + fileName);
            return false;
        }

        try {
            this.inputBuffer = (Batch) in.readObject();
            in.close();
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("Hash Join :Error in temporary file reading === " + fileName);
            }
        } catch (ClassNotFoundException c) {
            System.out.println("Hash Join :Some error in deserialization  === " + fileName);
            System.exit(1);
        } catch (IOException io) {
            System.out.println("Hash Join :temporary file reading error  === " + fileName);
            System.exit(1);
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
                    partition.clear();
                }
                partition.insertElementAt(t, partition.size());
            }
        }

        // Write out all non-full partitions
        for (int i = 0; i < partitions.length; i++) {
            Batch partition = partitions[i];
            if (partition.isEmpty()) {
                continue;
            }
            String fileName = generateFileName(i, partitionsCount[i], isLeft);
            pageWrite(partition, fileName);
            partitionsCount[i]++;
        }
        return true;
    }

    private boolean buildHashTable(int partitionNum) {

        // Array out of bound
        if (partitionNum >= partitionLeftPageCounts.length) {
            return false;
        }

        int pageCount = partitionLeftPageCounts[partitionNum];
        if (pageCount < 1) {
            return false;
        }

        // Clean old hash table tuples
        for (int i = 0; i < numBuff - 2; i++) {
            hashTable[i].clear();
        }

        // Read all pages of the partition from left
        for (int p = 0; p < pageCount; p++) {
            // Read a page of a partition
            String fileName = generateFileName(partitionNum, p, true);
            if (!pageRead(fileName)) {
                // Error reading partition
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

    private Tuple findMatchInHashtable(Batch[] hashTable, int hashAttriIndex, Tuple tuple, int tupleIndex) {
        int hash = hashFn2(tuple, tupleIndex, hashTable.length);
        for (int i = lTupleCurs + 1; i < hashTable[hash].size(); i++) {
            Tuple tupleInHash = hashTable[hash].elementAt(i);
            if (tupleInHash.checkJoin(tuple, hashAttriIndex, tupleIndex)) {
                lTupleCurs = i;
                return tupleInHash;
            }
        }
        lTupleCurs = -1;
        return null;
    }

}







































