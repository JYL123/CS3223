/**
 * To project out the group by attributes from the result
 **/

package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

public class GroupBy extends Operator {

    Operator base;
    Vector attrSet;
    int batchsize;  // number of tuples per outbatch

    Batch outbatch;
    int numBuffer;

    static int filenum = -1;   // To get unique filenum for this operation
    int currentFileNum; // Hash Join instance unique file num (to be used to generate output file name)
    ArrayList<String> fileNames = new ArrayList<>(); // File names of all partitions generated during partition phase
    int pageCurs; // To resume page reading after each next

    /**
     * index of the attributes in the base operator
     * * that are to be group by
     **/
    int[] attrIndex;
    ArrayList<ArrayList<Integer>> passCounts;


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
        numBuffer = 3; // Min buffer required
        pageCurs = 0;
        currentFileNum = ++filenum;
        passCounts = new ArrayList<>();

        /** The followingl loop findouts the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrSet.size()];

        for (int i = 0; i < attrSet.size(); i++) {
            Attribute attr = (Attribute) attrSet.elementAt(i);
            int index = baseSchema.indexOf(attr);
            attrIndex[i] = index;
        }

        if (!base.open())
            return false;

        /*
         * Sorting phase - In-memory sorting
         * Post-condition: Generate sorted initial run
         */
        Batch[] buffers = new Batch[numBuffer];

        buffers[0] = base.next();
        ArrayList<Integer> runCounts = new ArrayList<>();
        // Pass 0, # of runs produced
        passCounts.add(runCounts);
        int initialRun = -1;

        while (buffers[0] != null) {
            initialRun++;
            // Fill all buffers
            for (int i = 1; i < buffers.length; i++) {
                buffers[i] = base.next();
                if (buffers[i] == null) {
                    break;
                }
            }

            // Extract all vectors from tuples in the batches
            // TreeSet sort and eliminate duplicates
            TreeSet<Vector> orderedVectors = new TreeSet<>(getComparator());
            for (int i = 0; i < buffers.length; i++) {
                if (buffers[i] == null) {
                    break;
                }

                Iterator<Tuple> it = buffers[i].getIterator();
                while (it.hasNext()) {
                    orderedVectors.add(getGroupByValues(it.next()));
                }
            }

            // Write initial runs
            Iterator<Vector> it = orderedVectors.iterator();
            buffers[0] = new Batch(batchsize);
            int pageNum = -1;
            while (it.hasNext()) {
                Tuple t = new Tuple(it.next());
                buffers[0].add(t);
                Debug.PPrint(t);

                if (buffers[0].isFull()) {
                    pageNum++;
                    pageWrite(buffers[0], generateFileName(0, initialRun, pageNum));
                    buffers[0].clear();
                }
            }
            if (!buffers[0].isEmpty()) {
                pageNum++;
                pageWrite(buffers[0], generateFileName(0, initialRun, pageNum));
            }
            runCounts.add(initialRun, pageNum + 1);

            buffers[0] = base.next();
        }

        /*
         * Sorting phase (into single run)
         */
        for (int pass = 0; pass < passCounts.size(); pass++) {
            // For each pass
            ArrayList<Integer> preRunCounts = passCounts.get(pass);
            if (preRunCounts.size() < 2) {
                // Single run achieved
                return true;
            }

            // Output buffer
            buffers[buffers.length - 1] = new Batch(batchsize);
            ArrayList<Integer> curRunCount = new ArrayList<>();
            passCounts.add(curRunCount);
            int runCurs = -1;
            int newRunCount = 0;

            while (runCurs + 1 < preRunCounts.size()) {
                // New run in progress
                // Each iterator used up a single batch page
                ArrayList<Iterator<Tuple>> its = new ArrayList<>(numBuffer - 1);

                for (int i = runCurs + 1; i < preRunCounts.size(); i++) {
                    if (its.size() == numBuffer - 1) {
                        break;
                    }
                    runCurs = i;
                    // Load all runs into b -1 buffers
                    Iterator<Tuple> it = new TupleIterator(generateFileNamePrefix(pass, runCurs),
                            preRunCounts.get(runCurs));
                    its.add(it);
                }

                int pagesGenerated = merge(its, buffers[buffers.length - 1], pass + 1, newRunCount);
                curRunCount.add(newRunCount, pagesGenerated);
                newRunCount++;
                its = new ArrayList<>(numBuffer - 1);
                // End of one new run
            }
            // End of a pass
        }

        return true;
    }

    /**
     * Read next tuple from operator
     */

    public Batch next() {
        this.outbatch = null;

        int pass = passCounts.size() - 1;
        int run = passCounts.get(pass).size() - 1;
        if (run > 0) {
            System.out.println("Runs not merged!");
        }
        int pages = passCounts.get(pass).get(run);

        if (pageCurs < pages) {
            // Each file read match with batch size
            pageRead(generateFileName(pass, run, pageCurs));
            pageCurs++;
        }

        return outbatch;
    }


    /**
     * Close the operator
     */
    public boolean close() {
        for (int i = 0; i < fileNames.size(); i++) {
            File f = new File(fileNames.get(i));
            f.delete();
        }

        if (base.close())
            return true;
        else
            return false;
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
            System.out.println("Group By : writing the temporary file error === " + fileName);
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
            this.outbatch = (Batch) in.readObject();
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
     * @return file name generated
     */
    private String generateFileName(int passNum, int runNum, int pageNum) {
        return String.format("GBtemp%d-pass%d-run%d.%d", currentFileNum, passNum, runNum, pageNum);
    }

    private String generateFileNamePrefix(int passNum, int runNum) {
        return String.format("GBtemp%d-pass%d-run%d.", currentFileNum, passNum, runNum);
    }

    private Comparator<Vector> getComparator() {
        return (x, y) -> {
            for (int i = 0; i < x.size(); i++) {
                Object leftdata = x.get(i);
                Object rightdata = y.get(i);
                int comp = 0;
                if (leftdata instanceof Integer) {
                    comp = ((Integer) leftdata).compareTo((Integer) rightdata);
                } else if (leftdata instanceof String) {
                    comp = ((String) leftdata).compareTo((String) rightdata);

                } else if (leftdata instanceof Float) {
                    comp = ((Float) leftdata).compareTo((Float) rightdata);
                } else {
                    System.out.println("Tuple: Unknown comparision of the tuples");
                    System.exit(1);
                    return 0;
                }
                // Attribute is not equal (differences found)
                // If Attribute is equal (continue to compare the next attribute)
                if (comp != 0) {
                    return comp;
                }
            }
            return 0;
        };
    }

    private int merge(ArrayList<Iterator<Tuple>> its, Batch output, int nextPass, int run) {
        int pageNum = -1;
        TreeMap<Vector, Integer> map = new TreeMap<>(getComparator());

        // Initial loading
        for (int i = 0; i < its.size(); i++) {
            Iterator<Tuple> it = its.get(i);
            while (it.hasNext()) {
                Vector v = it.next().data();
                if (!map.containsKey(v)) {
                    // No duplicate, go to next iterator
                    map.put(v, i);
                    break;
                }
            }
        }

        // Polling of tuples and replacing
        while (!map.isEmpty()) {
            Map.Entry<Vector, Integer> outputVector = map.pollFirstEntry();
            // Put in batch, write if full
            output.add(new Tuple(outputVector.getKey()));
            if (output.isFull()) {
                pageNum++;
                pageWrite(output, generateFileName(nextPass, run, pageNum));
                System.out.println(generateFileName(nextPass, run, pageNum));
                Debug.PPrint(output);
                output.clear();
            }
            // Replace with next tuple from that particular run
            int index = outputVector.getValue();
            Iterator<Tuple> it = its.get(index);
            while (it.hasNext()) {
                Vector v = it.next().data();
                if (!map.containsKey(v)) {
                    // No duplicate, go to next iterator
                    map.put(v, index);
                    break;
                }
            }
        }

        if (!output.isEmpty()) {
            pageNum++;
            pageWrite(output, generateFileName(nextPass, run, pageNum));
            System.out.println(generateFileName(nextPass, run, pageNum));
            Debug.PPrint(output);
            output.clear();
        }

        return pageNum + 1;
    }
}
