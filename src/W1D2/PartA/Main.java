/* *****************************************************************
 * @SID      610575
 * @Author   ERDENEBAYAR CHINBAT
 * @Created  Oct 29, 2019
 ******************************************************************/

package W1D2.PartA;

import java.io.File;
import java.util.List;
import java.util.Scanner;
import java.io.FileNotFoundException;
import static W1D2.PartA.Mapper.map;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {
        List<Pair> mappedWords = null;
        List<GroupByPair> reducerInput = null;
        Scanner text = new Scanner(new File("./src/W1D1/testDataForW1D1.txt"));
        while (text.hasNextLine())
            mappedWords = map(text.next().toLowerCase());
        text.close();

        System.out.println("Mapper Output");
        //noinspection ConstantConditions
        mappedWords.forEach(System.out::println);

        System.out.println("Reducer Input");
//        reducerInput.add(mappedWords);
    }
}