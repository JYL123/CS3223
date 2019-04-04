/**
 * Scans the base relational table
 **/


package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.Vector;

/** Scan operator - read data from a file */

public class PartitionScan extends Operator {

    String fileNamePrefix;  //corresponding file name
    int filePageCount;  //tablename

    Batch inputBuffer;
    int pageCur;

    /** Constructor - just save filename  */

    public PartitionScan(String fileNamePrefix, int count, int type) {
        super(type);
        this.fileNamePrefix = fileNamePrefix;
        this.filePageCount = count;
    }


    /** Open file prepare a stream pointer to read input file */

    public boolean open() {
        /** num of tuples per batch**/
        pageCur = 0;
        return true;
    }


    public Batch next() {
        this.inputBuffer = null;
        if (pageCur < filePageCount) {
            pageRead(fileNamePrefix + pageCur);
            pageCur++;
        }
        return this.inputBuffer;
    }

    /** Close the file.. This routine is called when the end of filed
     ** is already reached
     **/
    public boolean close() {
        inputBuffer = null;
        return true;
    }


    public Object clone() {
        PartitionScan pscan = new PartitionScan(this.fileNamePrefix, this.filePageCount, this.optype);
        pscan.setSchema((Schema) schema.clone());
        return pscan;
    }

    private boolean pageRead(String fileName) {
        ObjectInputStream in;
        try {
            in = new ObjectInputStream(new FileInputStream(fileName));
            System.out.println(fileName);
        } catch (IOException io) {
            System.err.println("Partition Scan: error in reading the file === " + fileName);
            return false;
        }

        try {
            this.inputBuffer = (Batch) in.readObject();
            in.close();
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("Partition Scan:Error in temporary file reading === " + fileName);
            }
        } catch (ClassNotFoundException c) {
            System.out.println("Partition Scan:Some error in deserialization  === " + fileName);
            System.exit(1);
        } catch (IOException io) {
            System.out.println("Partition Scan:temporary file reading error  === " + fileName);
            System.exit(1);
        }

        return true;
    }
}
