package config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan({"services", "config"})
@PropertySource("classpath:user.properties")
public class AppConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .appName("spark-study")
                .master("local[*]")
                .getOrCreate();
    }

    @Bean
    public JavaSparkContext sc() {
        var sc = new JavaSparkContext(sparkSession().sparkContext());
        sc.setLogLevel("ERROR");
        return sc;
    }

    @Bean
    public String resourcesPath() {
        return "src/main/resources/";
    }
}
