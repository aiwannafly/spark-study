package services;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ArtistJudgeImpl implements ArtistJudge {
    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private PopularWordsService popularWordsService;

    @Autowired
    private String resourcesPath;

    @Override
    public List<String> topX(String artist, int x) {
        JavaRDD<String> textSongs = sc.textFile(resourcesPath + "data/songs/" + artist + "/*");
        return popularWordsService.topX(textSongs, x);
    }
}
