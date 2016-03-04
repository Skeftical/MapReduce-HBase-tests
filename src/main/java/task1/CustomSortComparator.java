package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by fotis on 10/02/16.
 */
public class CustomSortComparator extends WritableComparator {

    public CustomSortComparator(){
        super(CustomPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CustomPair pair1 = (CustomPair) a;
        CustomPair pair2 = (CustomPair) b;
        return pair1.compareTo(pair2);
    }
}
