/* *****************************************************************
 * @SID      610575
 * @Author   ERDENEBAYAR CHINBAT
 * @Created  Oct 28, 2019
 ******************************************************************/

package W1D1;

public class Pair<K, V>{
    private K key;
    private V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "< " + key + " , " + value + " >";
    }
}