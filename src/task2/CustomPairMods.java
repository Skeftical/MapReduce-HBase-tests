package task2;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by fotis on 06/02/16.
 */
public class CustomPairMods implements WritableComparable<CustomPairMods> {
    private int articleId;
    private int modifications;

    public int getArticleId() {
        return articleId;
    }



    public int getModifications() {
        return modifications;
    }

    public CustomPairMods(int articleId, int modifications) {
        this.articleId = articleId;
        this.modifications = modifications;
    }

    @Override
    public String toString() {
        return "CustomPairMods{" +
                "articleId=" + articleId +
                ", modifications=" + modifications +
                '}';
    }



    public int compareTo(CustomPairMods o) {
        if (this.modifications == o.getModifications()){
            return this.articleId > o.getArticleId() ? 1 : -1 ;
        }
        return this.modifications > o.getModifications() ? 1 : -1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(articleId);
        dataOutput.writeInt(modifications);
    }

    public void readFields(DataInput dataInput) throws IOException {
        articleId = dataInput.readInt();
        modifications = dataInput.readInt();
    }
}
