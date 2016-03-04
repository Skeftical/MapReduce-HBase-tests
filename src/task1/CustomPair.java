package task1;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CustomPair implements WritableComparable<CustomPair> {
    private Long articleId;
    private long revisionId;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(articleId);
        dataOutput.writeLong(revisionId);
    }
    public void readFields(DataInput dataInput) throws IOException {
        articleId = dataInput.readLong();
        revisionId = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "task1.CustomPair{" +
                "articleId=" + articleId +
                ", revisionId=" + revisionId +
                '}';
    }

    public void setArticleId(Long articleId) {
        this.articleId = articleId;
    }

    public void setRevisionId(long revisionId) {
        this.revisionId = revisionId;
    }

    public Long getArticleId() {
        return articleId;
    }

    public long getRevisionId() {
        return revisionId;
    }

    /**
     *From WritableComparable class
     * Note that hashCode() is frequently used in Hadoop to partition keys. It's important that your implementation of hashCode()
     * returns the same result across different instances of the JVM. Note also that the default hashCode()
     * implementation in Object does not satisfy this property.
     * @return
     */
    public int hashCode(){
        return articleId.hashCode();
    }

    public int compareTo(CustomPair o) {
        if (this.articleId.equals(o.articleId))
            return this.revisionId > o.revisionId ? 1 : -1;
        return this.articleId > o.articleId ? 1 : -1;
    }
}
