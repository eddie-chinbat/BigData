/* *****************************************************************
 * @SID      610575
 * @Author   ERDENEBAYAR CHINBAT
 * @Created  Oct 28, 2019
 ******************************************************************/

package W1D2.PartB;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Mapper {
    public static List<Pair> words = new ArrayList<>();

    public static List<Pair> map(String word){
        Pattern pattern = Pattern.compile("[-]|[,]|[.]$|[\\d]|[\"]|[_]|[!]|[?]");
        Matcher matcher;

        matcher = pattern.matcher(word);
        if(matcher.find()) {
            if(word.contains("-")) {
                for(String s: word.split("-"))
                    words.add(new Pair<String, Integer>(s, 1));
            }
            else if(word.contains("\""))
                words.add(new Pair<String, Integer>(word.split("\"")[1], 1));
            else if(word.matches("[a-z.]*|[a-z,]*|[a-z?]*|[a-z!]*"))
                words.add(new Pair<String, Integer>(word.substring(0,  word.length() - 1), 1));
        }
        else
            words.add(new Pair<String, Integer>(word, 1));

        Comparator comparator = new Comparator<Pair<String, Integer>>() {
            @Override
            public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        };

        //noinspection unchecked
        return (List<Pair>) words.stream()
                .sorted(comparator)
                .collect(Collectors.toList());
    }
}