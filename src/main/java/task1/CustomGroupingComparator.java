package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * Created by fotis on 04/02/16.
 */
public class CustomGroupingComparator extends WritableComparator {

    public CustomGroupingComparator(){
        super(CustomPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CustomPair pair1 = (CustomPair) a;
        CustomPair pair2 = (CustomPair) b;
        return pair1.getArticleId().compareTo(pair2.getArticleId());
    }
}
