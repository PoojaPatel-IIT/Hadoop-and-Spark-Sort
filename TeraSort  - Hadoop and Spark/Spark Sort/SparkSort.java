import org.apache.spark.api.java.JavaRDD;
import java.util.ArrayList;
import org.apache.spark.api.java.function.PairFunction;
import java.util.List;
import scala.Tuple2;
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
public class SparkSort {
public static void main(String[] args) {
	String war = "res";
	@SuppressWarnings({"res","war"})
	Long spark_time_starts = System.currentTimeMillis();
	String spark_sort = "Sorting using Spark";
	JavaSparkContext java_spark_context = new JavaSparkContext("local[*]", spark_sort);
	Long progStartTime = System.currentTimeMillis();	
	JavaRDD<String> file_ln;
	JavaPairRDD<String, String> file_sorted_result ;	
	file_ln= java_spark_context.textFile(args[0]);
	file_sorted_result = file_ln.mapToPair(new separates_key_pair()).sortByKey(true);      		
	file_sorted_result.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
			public Iterator<String> call(Tuple2<String, String> str_tup) throws Exception {
			boolean ret = true;
			ArrayList<String> val_returned_by_ArrayList = new ArrayList<String>();			
			String p = str_tup._1()+"  "+ str_tup._2().trim()+"\r";
			val_returned_by_ArrayList.add(p);								
			if(ret == false) {				
			System.out.println(val_returned_by_ArrayList);
			}
			return val_returned_by_ArrayList.iterator();
		}
	}).saveAsTextFile(args[1]);
	Long spark_time_ends = System.currentTimeMillis();
	Long computation_spark_time;
	computation_spark_time = spark_time_ends - spark_time_starts;
	float tot_time_spark = computation_spark_time/1000;
	System.out.println( "Total Computation time (sec) for Spark "+ tot_time_spark + "sec");
}
private static class separates_key_pair implements PairFunction<String, String, String> {
	public Tuple2<String, String> call(String strn) {
		String first_10_bytes_key = "";
		first_10_bytes_key += strn.substring(0, 10);
		String next_bytes_values = "" ;
		next_bytes_values += strn.substring(10);
		return new Tuple2<String, String>(first_10_bytes_key, next_bytes_values);
	}	
}
}