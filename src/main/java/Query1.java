import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.*;
import utils.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Query1 {
   // private static final String pathToFile = "data/prj1_dataset/weather_description.csv";

    private static int minHoursIsClearForDay = 18;
    private static int minDayIsClearForMonth = 15;
    private static List<Integer> desiredMonths = new ArrayList<>(Arrays.asList(3,4,5));
    private static final String weatherCondition= "sky is clear";


    public static void main(String[] args) {

        String pathToFile= args[0];

        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Log Analyzer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //read file
        JavaRDD<String> file = sc.textFile(pathToFile);
        String header = file.first();
        String[] firstLine = header.split(",",-1);

        //get city names and timezones
        String[] cityNames;
        cityNames= Arrays.copyOfRange(firstLine, 1, firstLine.length);
        String[] finalCityNames = cityNames;
        GetterInfo getterInfo = new GetterInfo();
        String[] timeZones = getterInfo.getTimeZoneFromCityName(finalCityNames);

        //get ther other lines of csv file
        JavaRDD<String> otherLines = file.filter(row -> !row.equals(header));
        JavaRDD<ArrayList<City>>  listOflistOfcities = otherLines
                .map(line -> CityParser.parseCSV(line, finalCityNames,timeZones));

        //convert to tuple5 City-Year-Month-Day-Weather_description
        JavaRDD<Tuple5<String, Integer,Integer,Integer, String>> citiesParsed = listOflistOfcities
                .flatMap(new ParseRDDofLists())
                .filter(x-> desiredMonths.contains(x._3())); //take only desired month

        //check  number of hour in a day with sky is clear
        JavaPairRDD<Tuple4<String, Integer,Integer,Integer>, Integer> daySkyIsClear = citiesParsed
                .mapToPair(new ControlHour())
                .filter(x->x!=null)
                .reduceByKey((x,y) -> x+y)
                .filter(day->day._2>=minHoursIsClearForDay); //control number of hour with sky is clear

        //check number of day in a month with sky is clear
        JavaRDD<Tuple3<String, Integer,Integer>> monthIsClear = daySkyIsClear
                .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1._1(),x._1._2(),x._1._3()), 1))
                .reduceByKey((x,y)->x+y)
                .filter(month -> month._2>=minDayIsClearForMonth)
                .map(x-> new Tuple3<>(x._1._1(),x._1._2(),x._1._3()));

        //check months for each year and return for each year the cities
        JavaPairRDD<Integer, Iterable<String>> yearIsClear = monthIsClear
                .mapToPair(x-> new Tuple2<>(new Tuple2<>(x._1(),x._2()),x._3()))
                .groupByKey()
                .filter(x-> StreamSupport.stream(x._2().spliterator(), false)
                        .collect(Collectors.toList()).containsAll(desiredMonths))
                .mapToPair(x-> new Tuple2<>(x._1._2(), x._1._1()))
                .distinct()
                .groupByKey()
                .sortByKey();

        //print final result
        List<Tuple2<Integer,Iterable<String>>> finalResult = yearIsClear.collect();

        for (Tuple2<Integer,Iterable<String>> r : finalResult){
            Iterator<String> iterator = r._2().iterator();
            System.out.print("Year: " + r._1()
                    + "  Cities: <  ");
            while (iterator.hasNext()) {
                System.out.print(iterator.next()+"   ");
            }
            System.out.println(">\n");

        }

        sc.stop();

    }


    private static class ParseRDDofLists implements FlatMapFunction<ArrayList<City>, Tuple5<String, Integer,Integer,Integer, String> > {
        @Override
        public Iterator<Tuple5<String, Integer, Integer, Integer, String>> call(ArrayList<City> cities) {
            List<Tuple5<String, Integer, Integer, Integer, String>> results= new ArrayList<>();
            for (City city : cities) {
                Tuple5<String, Integer, Integer, Integer, String> result = new Tuple5<>(city.getCity(), city.getYear(),
                        city.getMonth(), city.getDay(), city.getValue());
                results.add(result);
            }
            return results.iterator();
        }
    }


    private static class ControlHour implements PairFunction<Tuple5<String, Integer, Integer, Integer, String>,
                                                      Tuple4<String, Integer, Integer, Integer>, Integer> {
        @Override
        public Tuple2<Tuple4<String, Integer,Integer,Integer>, Integer> call(Tuple5<String, Integer, Integer, Integer, String> stringIntegerIntegerIntegerStringTuple5) {
            Tuple2<Tuple4<String, Integer,Integer,Integer>, Integer> result = null;
            Tuple5<String, Integer, Integer, Integer, String> x= stringIntegerIntegerIntegerStringTuple5;

            if(x._5().equals(weatherCondition))
                result = new Tuple2<>(new Tuple4<>(x._1(),x._2(),x._3(),x._4()), 1);

            return result;
        }
    }


}
