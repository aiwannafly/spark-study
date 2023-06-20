package services;

import config.UserConfig;
import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Service
public class PopularWordsServiceImpl implements PopularWordsService {
    @Autowired
    private UserConfig userConfig;

    @Override
    public List<String> topX(JavaRDD<String> lines, int x) {
        System.out.println(userConfig.garbage);
        return lines
                .map(String::toLowerCase)
                .flatMap(PopularWordsServiceImpl::getWords)
//                .filter(s -> !userConfig.garbage.contains(s))
                .mapToPair(w -> new Tuple2<>(w, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2)
                .take(x);
    }

    public static Iterator<String> getWords(String line) {
        return Arrays.stream(line.split(" ")).iterator();
    }
}
