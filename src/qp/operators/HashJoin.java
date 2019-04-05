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
import java.util.List;
import java.util.Objects;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.RandNumb;
import qp.utils.Tuple;

public class HashJoin extends Join {

    /**
     * The following fields are useful during execution of
     * * the Hash Join operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    Batch inputBuffer;
    Batch outputBuffer; // Used in probing phase
    Batch[] hashTable; // Build in probing phase

    final int PRIME1;
    final int PRIME2;
    final int PRIME3;
    final int PRIME4;

    static int filenum = -1;   // To get unique filenum for this operation
    int currentFileNum; // Hash Join instance unique file num (to be used to generate output file name)
    ArrayList<String> fileNames = new ArrayList<>(); // File names of all partitions generated during partition phase
    int[] partitionLeftPageCounts, partitionsRightPageCounts; // File count per partition
    List<Integer> failedPartition = new ArrayList<>();
    int recusiveCount;

    int partitionCurs; // Current partition cursor
    int rPageCurs; // Current right page cursor
    int rTupleCurs; // Last probed tuple in the right page
    int lTupleCurs; // Last checked tuple in a particular bucket of the hash table
    boolean done;

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        recusiveCount = 0;
        PRIME1 = RandNumb.randPrime();
        int tempPrime = RandNumb.randPrime();
        while (PRIME1 == tempPrime) {
            tempPrime = RandNumb.randPrime();
        }
        PRIME2 = tempPrime;
        while (PRIME1 == tempPrime || PRIME2 == tempPrime) {
            tempPrime = RandNumb.randPrime();
        }
        PRIME3 = tempPrime;
        while (PRIME1 == tempPrime || PRIME2 == tempPrime || PRIME3 == tempPrime) {
            tempPrime = RandNumb.randPrime();
        }
        PRIME4 = tempPrime;

    }

    public HashJoin(Join jn, int recusiveCount) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        this.recusiveCount = recusiveCount;
        PRIME1 = RandNumb.randPrime();
        int tempPrime = RandNumb.randPrime();
        while (PRIME1 == tempPrime) {
            tempPrime = RandNumb.randPrime();
        }
        PRIME2 = tempPrime;
        while (PRIME1 == tempPrime || PRIME2 == tempPrime) {
            tempPrime = RandNumb.randPrime();
        }
        PRIME3 = tempPrime;
        while (PRIME1 == tempPrime || PRIME2 == tempPrime || PRIME3 == tempPrime) {
            tempPrime = RandNumb.randPrime();
        }
        PRIME4 = tempPrime;
    }

    public boolean open() {
        currentFileNum = ++filenum;
        Attribute leftattr = con.getLhs();
        Attribute rightattr = (Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        // Partition buffers
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

        // Setup Buffer for probing phase's hash table
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
        done = false;
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
        if (done) {
            System.out.println("DONE");
            return null;
        }

        // Prepare (clean) output buffer for new set of joined tuples
        outputBuffer.clear();

        for (int parti = partitionCurs; parti < partitionLeftPageCounts.length && parti >= 0; parti++) {
            // BUILDING PHASE
            if (parti != partitionCurs) {
                // Probing partition[parti], but hash table (is at partitionCurs) not updated
                if (buildHashTable(parti)) {
                    partitionCurs = parti;
                } else {
                    // Error reading partition or no tuple in the left of this partition
                    // Move to next partition to build hash table
                    partitionCurs = parti;
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
                // End of page, reset tuple curs
                rTupleCurs = -1;
            }
            // End of partition, reset page and tuple curs
            rPageCurs = 0;
            rTupleCurs = -1;
        }
        partitionCurs = Integer.MAX_VALUE;
        if (!outputBuffer.isEmpty()) {
            return outputBuffer;
        }

        // End of probing
        if (recursiveHashJoin()) {
            return outputBuffer;
        }

        done = true;
        close();
        return null;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        System.out.println("CLOSE!!! " + filenum);
        // Delete all partition files
        for (int i = 0; i < fileNames.size(); i++) {
            File f = new File(fileNames.get(i));
            f.delete();
        }

//        inputBuffer.clear();
        inputBuffer = null;
//        for (int i = 0; i < numBuff - 2; i++) {
//            hashTable[i].clear();
//        }
        hashTable = null;

        return true;
    }

    /**
     * Hash function used for partitioning.
     *
     * @param t          the tuple
     * @param index      the index of the attribute to be hashed
     * @param bucketSize the size of the partition
     * @return partition to be hashed
     */
    private int hashFn1(Tuple t, int index, int bucketSize) {
        Object value = t.dataAt(index);
        int h = Objects.hash(value);
        return ((h * PRIME1) % PRIME2) % bucketSize;
    }

    /**
     * Hash function used for probing.
     *
     * @param t          the tuple
     * @param index      the index of the attribute to be hashed
     * @param bucketSize the size of the buckets in the hash table
     * @return bucket to be hashed
     */
    private int hashFn2(Tuple t, int index, int bucketSize) {
        Object value = t.dataAt(index);
        int h = Objects.hash(value);
        return ((h * PRIME3) % PRIME4) % bucketSize;
//        return (h * PRIME2) % bucketSize;
    }

    /**
     * Batch write out.
     *
     * @param b        page to be written out
     * @param fileName name of the file
     */
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

    /**
     * Read batch into input buffer.
     *
     * @param fileName name of the file to be read
     * @return True if page has been read into input buffer
     */
    private boolean pageRead(String fileName) {
        ObjectInputStream in;
        try {
            in = new ObjectInputStream(new FileInputStream(fileName));
            System.out.println(fileName);
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

    /**
     * File name generator to standardize file name used in I/O
     *
     * @param partitionNum the partition of the tuple hashed
     * @param pageNum      running page number from 0 to end of batch of the partition
     * @param isLeft       the original operator of the tuples in stored file
     * @return file name generated
     */
    private String generateFileName(int partitionNum, int pageNum, boolean isLeft) {
        if (isLeft) {
            return String.format("HJtemp%d-LeftPartition%d-%d", currentFileNum, partitionNum,
                    pageNum);
        } else {
            return String.format("HJtemp%d-RightPartition%d-%d", currentFileNum, partitionNum,
                    pageNum);
        }

    }

    private String generateFileNamePrefix(int partitionNum, boolean isLeft) {
        if (isLeft) {
            return String.format("HJtemp%d-LeftPartition%d-", currentFileNum, partitionNum);
        } else {
            return String.format("HJtemp%d-RightPartition%d-", currentFileNum, partitionNum);
        }

    }

    /**
     * Generate partitions of a operator (left or right) during partitioning phase.
     *
     * @param partitions      output buffers (each represent a paritition to be written when full)
     * @param partitionsCount Count of each partition (to be updated when partition count increases)
     * @param op              the operator to be partitioned
     * @param index           the index of the attribute to be hashed
     * @return true if partition is successful, or else false
     */
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

        return op.close();

    }

    /**
     * Build hash table for a specific partition in preparation for probing.
     *
     * @param partitionNum the partition to build the hash table for
     * @return True if hash table is built successfully, else false
     */
    private boolean buildHashTable(int partitionNum) {

        // Array out of bound, ensures partition exist
        if (partitionNum >= partitionLeftPageCounts.length) {
            return false;
        }

        int pageCount = partitionLeftPageCounts[partitionNum];
        if (pageCount < 1) {
            // No records in partition to build hash table
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
                    failedPartition.add(partitionNum);
                    System.out.printf("Hash table (bucket: %d) is too small\n", hash);
                    return false;
                }
                hashTable[hash].insertElementAt(tuple, hashTable[hash].size());
            }
        }
        return true;
    }

    /**
     * Find match in a bucket of the hash table.
     *
     * @param hashTable      the hash table to be search in
     * @param hashAttriIndex attribute index to be match against tuple in the hash table
     * @param tuple          search tuple
     * @param tupleIndex     attribute index to be match against for the tuple
     * @return matching joined tuple
     */
    private Tuple findMatchInHashtable(Batch[] hashTable, int hashAttriIndex, Tuple tuple, int tupleIndex) {
        int hash = hashFn2(tuple, tupleIndex, hashTable.length);

        // Search for match in a specifc bucket of the hash table
        for (int i = lTupleCurs + 1; i < hashTable[hash].size(); i++) {
            Tuple tupleInHash = hashTable[hash].elementAt(i);
            lTupleCurs = i;
            System.out.printf("%d - Partition: %d/%d, Right-page: %d/%d, Right-tuple: %d/%d, Left-tuple: %d/%d",
                    filenum, partitionCurs,
                    partitionLeftPageCounts.length - 1, rPageCurs, partitionsRightPageCounts[partitionCurs] - 1,
                    rTupleCurs,
                    inputBuffer.size() - 1, lTupleCurs, hashTable[hash].size() - 1);
            if (tupleInHash.checkJoin(tuple, hashAttriIndex, tupleIndex)) {
                // Return back to curs to continue search
                System.out.print(" == FOUND!");
                System.out.println(" ");
                return tupleInHash;
            } else {
                System.out.println(" ");
            }
        }
        // End of search, reset curs
        lTupleCurs = -1;
        return null;
    }

    Join recursiveJoin;

    private boolean recursiveHashJoin() {
        while (failedPartition.size() > 0) {
            int failedPartiNum = failedPartition.get(0);
            if (recursiveJoin == null) {
                if (recusiveCount > 3) {
                    System.out.println("3 recursive hash joins has encountered lack of buffer in hash table. " +
                            "Partition will be merged using block nested loop instead.");
                    setupBJ(failedPartiNum);
                } else {
                    setupRecursiveHJ(failedPartiNum);
                }
            }

            this.outputBuffer = recursiveJoin.next();
            if (this.outputBuffer == null) {
                failedPartition.remove(0);
                recursiveJoin.close();
                recursiveJoin = null;
            } else {
                // Join tuples found
                return true;
            }
        }
        return false;
    }

    private void setupRecursiveHJ(int failedPartiNum) {
        PartitionScan s1 = new PartitionScan(generateFileNamePrefix(failedPartiNum, true),
                partitionLeftPageCounts[failedPartiNum], OpType.SCAN);
        s1.setSchema(left.schema);
        PartitionScan s2 = new PartitionScan(generateFileNamePrefix(failedPartiNum, false),
                partitionsRightPageCounts[failedPartiNum], OpType.SCAN);
        s2.setSchema(right.schema);
        Join j = new Join(s1, s2, this.con, this.optype);
        j.setSchema(schema);
        j.setJoinType(getJoinType());
        j.setNumBuff(numBuff);
        recursiveJoin = new HashJoin(j, recusiveCount + 1);
        recursiveJoin.open();
    }

    private void setupBJ(int failedPartiNum) {
        PartitionScan s1 = new PartitionScan(generateFileNamePrefix(failedPartiNum, true),
                partitionLeftPageCounts[failedPartiNum], OpType.SCAN);
        s1.setSchema(left.schema);
        PartitionScan s2 = new PartitionScan(generateFileNamePrefix(failedPartiNum, false),
                partitionsRightPageCounts[failedPartiNum], OpType.SCAN);
        s2.setSchema(right.schema);
        Join j = new Join(s1, s2, this.con, this.optype);
        j.setSchema(schema);
        j.setJoinType(JoinType.NESTEDJOIN);
        j.setNumBuff(numBuff);
        recursiveJoin = new NestedJoin(j);
        recursiveJoin.open();
    }

}







































