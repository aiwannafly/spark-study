package services;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public interface PopularWordsService {

    List<String> topX(JavaRDD<String> lines, int x);
}
