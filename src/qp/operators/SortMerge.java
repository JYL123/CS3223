package qp.operators;

import org.ietf.jgss.Oid;
import qp.utils.*;

import java.io.*;
import java.util.*;

import static qp.utils.Tuple.compareTuples;

public class SortMerge extends Operator {

    Operator base;
    Vector attrSet;
    int batchsize;

    int[] attrIndex;
    int numBuff;
    List<File> sortedFiles;

    ObjectInputStream in;
    int numRuns;

    boolean eos;



    public SortMerge(Operator base, Vector attrSet, int opType) {
        super(opType);
        this.base = base;
        this.attrSet = attrSet;
    }

    public SortMerge(Operator base, Vector attrSet) {
        super(OpType.SORT);
        this.base = base;
        this.attrSet = attrSet;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setNumBuff(int numBuff) {
        this.numBuff = numBuff;
    }

    public int getNumBuff() {
        return numBuff;
    }

    /** opens the connection to the base operator **/
    public boolean open() {

        if (numBuff < 3) {
            System.out.println("Error: Min number of buffer required is 3");
            System.exit(1);
        }

        /* set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];
        for (int i = 0; i < attrSet.size(); i++) {
            Attribute a = (Attribute) attrSet.elementAt(i);
            System.out.println("\nSORT attribute: -------------------------------");
            Debug.PPrint(a);
            int id = baseSchema.indexOf(a);
            attrIndex[i] = id;
        }

        if (base.open()) {
            /** generate sorted runs **/
            System.out.println("~~~~~~~~~~~~~~~~~` Generate sorted runs");
            sortedFiles = new ArrayList<File>();
            numRuns = 0;
            /** read the tuples in buffer as much as possible **/
            Batch inBatch = base.next();
            while (inBatch != null && !inBatch.isEmpty()) {
                Block run = new Block(numBuff, batchsize);
                while (inBatch != null && !inBatch.isEmpty() && !run.isFull()) {
                    run.addBatch(inBatch);
                    inBatch = base.next();
                }
                numRuns++;
                Vector<Tuple> tuples = run.getTuples();
                Collections.sort(tuples, new AttrComparator(attrIndex));

                Block sortedRun = new Block(numBuff, batchsize);
                sortedRun.setTuples((Vector) tuples);
                File f = writeToFile(sortedRun, numRuns);
                sortedFiles.add(f);
            }

            /** merge sorted runs **/
            System.out.println("~~~~~~~~~~~~~~~~~` Merge sorted runs");
            mergeSortedFiles();


            try {
                in = new ObjectInputStream(new FileInputStream(sortedFiles.get(0)));
            } catch (FileNotFoundException e) {
                System.out.println("Error: file " + sortedFiles.get(0) + " not found");
                return false;
            } catch (IOException e) {
                System.out.println("Error: cannot read file " + sortedFiles.get(0));
                return false;
            }
            return true;
        }
        else {
            return false;
        }
    }


    /** returns a batch of tuples **/
    public Batch next() {
        if (sortedFiles.size() != 1) {
            System.out.println("Error: incorrectly sorted");
        }
        try {
            Batch b = (Batch) in.readObject();
            return b;
        } catch (IOException e) {
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("Error: file not found");
        }
        return null;
    }


    /** closes the operator **/
    public boolean close() {
        for (File f: sortedFiles) {
            f.delete();
        }
        try {
            in.close();
        } catch (IOException e) {
            System.out.println("Error: cannot close input stream");
        }
        return true;
    }



    /** recursively merge until the last run is left **/
    private void mergeSortedFiles() {
        int numInputBuffer = numBuff - 1;
        int instanceNumber = 0;
        List<File> resultSortedFiles;
        while(sortedFiles.size() > 1) {
            resultSortedFiles = new ArrayList<>();
            int numMergeRuns = 0;
            for (int i = 0; i * numInputBuffer < sortedFiles.size(); i++) {
                // number of files being sorted equals to the number of input buffers
                int start = i * numInputBuffer;
                int end = start + numInputBuffer;
                if (end >= sortedFiles.size()) { // the last run may not use up all the input buffers
                    end = sortedFiles.size();
                }
                List<File> filesToBeSorted = sortedFiles.subList(start, end);
                File singleSortedFile = mergeSortedRuns(filesToBeSorted, instanceNumber, numMergeRuns);

                numMergeRuns++;
                resultSortedFiles.add(singleSortedFile);
            }

            for (File f: sortedFiles) {
                f.delete();
            }
            sortedFiles = resultSortedFiles;
            instanceNumber++;
        }
    }

    private File mergeSortedRuns(List<File> runs, int instanceNumber, int numMergeRuns) {
        int runIndex; // a cursor pointing at the index of list of runs
        int numInputBuffer = numBuff - 1;
        boolean needsAdditionalBuff = (numInputBuffer > numRuns);

        if (numInputBuffer < runs.size()) {
            System.out.println("Error: number of runs exceeds capacity of input buffer. Sorting terminates.");
            return null;
        }

        ArrayList<ObjectInputStream> inputStreams = new ArrayList<>();
        try {
            for (int i = 0; i < runs.size(); i++) {
                ObjectInputStream inputStream = new ObjectInputStream((new FileInputStream(runs.get(i))));
                inputStreams.add(inputStream);
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error: file not found");
        } catch (IOException e) {
            System.out.println("Error: cannot open/write to temp file");
        }

        /** start MERGING **/
        File resultFile = new File("Run-" + instanceNumber + "-" + numMergeRuns);
        ObjectOutputStream out = initObjectOutputStream(resultFile);


        ArrayList<Batch> inBuffers = new ArrayList<>(runs.size());
        for (int i = 0; i < runs.size(); i++) {
            Batch b = getNextBatch(inputStreams.get(i));
            if (b == null) {
                System.out.println("Merging Error: Run-" + i + " is empty");
            }
            inBuffers.add(i, b);
        }






        /** write results to output stream **/
        Batch outBuffer = new Batch(batchsize);
        Batch temp;

        if(needsAdditionalBuff) {
            Queue<Tuple> inputTuples = new PriorityQueue<>(runs.size(), new AttrComparator(attrIndex));
            Map<Tuple, Integer> tupleRunIndexMap = new HashMap<>(runs.size());
            for (int j = 0; j < runs.size(); j++) {
                temp = inBuffers.get(j);
                Tuple t = temp.remove(0);

                inputTuples.add(t);
                tupleRunIndexMap.put(t, j);

                if (temp.isEmpty()) {
                    temp = getNextBatch(inputStreams.get(j));
                    inBuffers.set(j, temp);

                }
            }


            while (!inputTuples.isEmpty()) {
                Tuple minTuple = inputTuples.remove();
                outBuffer.add(minTuple);
                if (outBuffer.isFull()) { // write entries to the output buffer
                    writeToOutput(outBuffer, out);
                    outBuffer.clear();
                }

                // extract another tuple from the same run until there are no more tuples in this run
                // and add the tuple into the queue
                runIndex = tupleRunIndexMap.get(minTuple);
                temp = inBuffers.get(runIndex);
                if (temp != null) {
                    Tuple t = temp.remove(0);

                    inputTuples.add(t);
                    tupleRunIndexMap.put(t, runIndex);

                    if (temp.isEmpty()) {
                        temp = getNextBatch(inputStreams.get(runIndex));
                        inBuffers.set(runIndex, temp);
//                        if (temp == null) {
//                            System.out.println("Run-" + runIndex + " has been processed");
//                        }
                    }
                }
            }

            // add the remaining tuples in output buffer to output stream
            if (!outBuffer.isEmpty()) {
                writeToOutput(outBuffer, out);
                outBuffer.clear();
            }

            tupleRunIndexMap.clear();
        }
        else {
            while (!completesExtraction(inBuffers)) {
                runIndex = getIndexofMinTuple(inBuffers);
                temp = inBuffers.get(runIndex);

                // add minTuple to output buffer
                outBuffer.add(temp.remove(0));
                // write result in output buffer into out stream
                if (outBuffer.isFull()) {
                    writeToOutput(outBuffer, out);
                    outBuffer.clear();
                }

                if(temp.isEmpty()) {
                    temp = getNextBatch(inputStreams.get(runIndex));
                    inBuffers.set(runIndex, temp);
                }
            }

            // add the remaining tuples in output buffer to output stream
            if (!outBuffer.isEmpty()) {
                writeToOutput(outBuffer, out);
                outBuffer.clear();
            }

        }

        try {
            out.close();
        } catch (IOException e) {
            System.out.println("Error: unable to close output stream");
        }

        return resultFile;
    }

    private int getIndexofMinTuple(ArrayList<Batch> inBuffers) {
        Tuple minTuple = null;
        int index = 0;
        // get the first non-null tuple in the input buffer
        for (int i = 0; i < inBuffers.size(); i++) {
            if (inBuffers.get(i) != null) {
                minTuple = inBuffers.get(i).elementAt(0);
                index = i;
            }
        }
        // compare the entire input buffer to find the actual min
        for (int j = 0; j < inBuffers.size(); j++) {
            if (inBuffers.get(j) != null) {
                Tuple current = inBuffers.get(j).elementAt(0);
                int result = 0;
                for (int idx: attrIndex) {
                    result = Tuple.compareTuples(current, minTuple, index, index);
                    if (result != 0) {
                        break;
                    }
                }
                if(result < 0) {
                    minTuple = current;
                    index = j;
                }
            }
        }
        return index;
    }

    private boolean completesExtraction(ArrayList<Batch> inBuffers) {
        for (int i = 0; i < inBuffers.size(); i++) {
            if(inBuffers.get(i) != null) {
                return false;
            }
        }
        return true;
    }


    private void writeToOutput(Batch outBuffer, ObjectOutputStream out) {
        try {
            out.writeObject(outBuffer);
            out.reset();
        } catch (IOException e) {
            System.out.println("Sort Merge Error: cannot write to output steam");
        }
    }

    private ObjectOutputStream initObjectOutputStream(File resultFile) {
        try {
            return new ObjectOutputStream(new FileOutputStream(resultFile, true));
        } catch (FileNotFoundException e) {
            System.out.println("Error: file not found");
        } catch (IOException e) {
            System.out.println("Error: cannot open/write to temp file");
        }
        return null;
    }

    private Batch getNextBatch(ObjectInputStream in) {
        try {
            Batch b = (Batch) in.readObject();
            if (b.isEmpty()) {
                System.out.println("Note: empty batch");
            }
            return b;
        } catch (IOException e) {
            System.out.println("Error: cannot open/write to temp file");
            return null;
        } catch (ClassNotFoundException e) {
            System.out.println("Error: class not found");
        }
        return null;
    }

    private File writeToFile(Block sortedRun, int numRuns) {
        try {
            File temp = new File("SMTemp-" + numRuns);
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(temp));
            for (Batch b: sortedRun.getBatches()) {
                out.writeObject(b);
            }
            out.close();
            return temp;
        } catch (FileNotFoundException e) {
            System.out.println("Error: file not found");
        } catch (IOException e) {
            System.out.println("Error: cannot open/write to temp file");
        }
        return null;
    }
}
