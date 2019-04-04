package qp.utils;

import java.util.Comparator;

import static qp.utils.Tuple.compareTuples;

public class AttrComparator implements Comparator<Tuple> {
    private int[] attrIndex;

    public AttrComparator(int[] attrIndex) {
        this.attrIndex = attrIndex;
    }

    @Override
    public int compare(Tuple t1, Tuple t2) {
        int result;
        for (int index: attrIndex) {
            result = Tuple.compareTuples(t1, t2, index, index);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

}
