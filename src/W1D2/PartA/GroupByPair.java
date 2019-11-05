/* *****************************************************************
 * @SID      610575
 * @Author   ERDENEBAYAR CHINBAT
 * @Created  Oct 29, 2019
 ******************************************************************/

package W1D2.PartA;

import java.util.List;

public class GroupByPair<K, V>{
    private K key;
    private List<V> value;

    public GroupByPair(K key, List<V> value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public List<V> getValue() {
        return value;
    }

    public void setValue(List<V> value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "< " + key + " , [" + value + "] >";
    }
}