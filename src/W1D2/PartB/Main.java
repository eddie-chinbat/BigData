/* *****************************************************************
 * @SID      610575
 * @Author   ERDENEBAYAR CHINBAT
 * @Created  Oct 28, 2019
 ******************************************************************/

package W1D2.PartB;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

import static W1D2.PartB.Mapper.map;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {
        List<Pair> mappedWords = null;
        Scanner text = new Scanner(new File("./src/W1D1/testDataForW1D1.txt"));
        while (text.hasNextLine())
            mappedWords = map(text.next().toLowerCase());
        text.close();

        System.out.println("Mapper Output");
        //noinspection ConstantConditions
        mappedWords.forEach(System.out::println);
    }
}