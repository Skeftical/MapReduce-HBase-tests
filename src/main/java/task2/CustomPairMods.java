package task2;



/**
 * Created by fotis on 06/02/16.
 */
public class CustomPairMods implements Comparable<CustomPairMods> {
    private long articleId;
    private long modifications;

    public long getArticleId() {
        return articleId;
    }

    public long getModifications() {
        return modifications;
    }

    public CustomPairMods(long articleId, long modifications) {
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


    @Override
    public int compareTo(CustomPairMods o) {
        if (this.modifications == o.getModifications()){
            return this.articleId > o.getArticleId() ? 1 : -1 ;
        }
        return this.modifications > o.getModifications() ? 1 : -1;
    }

}
