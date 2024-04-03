package spark.batch.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import com.google.common.base.Preconditions;

public class CompanyCount {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompanyCount.class);

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new CompanyCount().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) {
        SparkConf conf = new SparkConf()
                .setAppName(CompanyCount.class.getName());
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        JavaPairRDD<String, Integer> industryCounts = textFile
                .mapToPair(line -> {
                    String industry = line.split("\t")[5];
                    return new Tuple2<>(industry, 1);
                })
                .reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> countsList = industryCounts.collect();

        for (Tuple2<String, Integer> industryCount : countsList) {
            LOGGER.info(industryCount._1() + "\t" + industryCount._2());
        }

        industryCounts.saveAsTextFile(outputDir);

        sc.stop();
    }
}
