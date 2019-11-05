/* *****************************************************************
 * @SID      610575
 * @Author   ERDENEBAYAR CHINBAT
 * @Created  Oct 31, 2019
 ******************************************************************/

package W1D3;

public class WordCount {
    private int i;
    private int j;

    public WordCount(int i, int j) {
        this.i = i;
        this.j = j;
    }

    public void process(String filepath) {
        System.out.println("Number of Mapper: " + 4);
        System.out.println("Number of Reducers: " + 3);
    }
}
