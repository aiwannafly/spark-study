package services;

import config.AppConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = AppConfig.class)
public class PopularWordsServiceImplTest {
    @Autowired
    SparkSession sparkSession;

    @Autowired
    PopularWordsService popularWordsService;

    @Test
    public void topX() {
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> lines = sc.parallelize(List.of("hi", "bye", "hi", "hi", "see", "see"));
        Assertions.assertEquals("hi", popularWordsService.topX(lines, 1).get(0));
    }
}
