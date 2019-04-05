/**
 * To project out the group by attributes from the result
 **/

package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

public class TupleIterator implements Iterator<Tuple> {
    String fileNamePrefix;
    int totalPageNum;
    int pageCurs;

    Batch buffer;
    Iterator<Tuple> it;

    public TupleIterator(String fileNamePrefix, int totalPageNum) {
        this.fileNamePrefix = fileNamePrefix;
        this.totalPageNum = totalPageNum;
        pageCurs = -1;
    }

    public boolean hasNext() {
        if (it == null || !it.hasNext())
            return loadNextPage();

        return it.hasNext();
    }

    public Tuple next() {
        if (hasNext()) {
            return it.next();
        } else {
            return null;
        }
    }

    private boolean loadNextPage() {
        if (pageCurs + 1 >= totalPageNum)
            return false;


        if (!pageRead(fileNamePrefix + (pageCurs + 1))) {
            return false;
        }
        pageCurs++;

        if (buffer.isEmpty()) {
            return false;
        }

        it = buffer.getIterator();
        return it.hasNext();
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
        } catch (IOException io) {
            System.err.println("TupleIterator: error in reading the file === " + fileName);
            return false;
        }

        try {
            buffer = (Batch) in.readObject();
            in.close();
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("TupleIterator: Error in temporary file reading === " + fileName);
            }
        } catch (ClassNotFoundException c) {
            System.out.println("TupleIterator: Some error in deserialization  === " + fileName);
            System.exit(1);
        } catch (IOException io) {
            System.out.println("TupleIterator: temporary file reading error  === " + fileName);
            System.exit(1);
        }

        return true;
    }

}

