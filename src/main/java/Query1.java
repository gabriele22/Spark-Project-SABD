import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.*;
import utils.*;
import java.lang.Long;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
/**
 this class (by default) finds ,for each year,
 the cities that have at least 15 days of clear weather
 in the months of March, April and May
 */
public class Query1 {
    private static int minHoursIsClearForDay = 18;
    private static int minDayIsClearForMonth = 15;
    private static List<Integer> desiredMonths = new ArrayList<>(Arrays.asList(3,4,5));
    private static final String weatherCondition= "sky is clear";


    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("\nERROR: Insert arguments in this order: " +
                    "1. 'file city-attributes, 2. file weather-description'");
        }
        long initialTime = System.currentTimeMillis();

        String pathFileCityAttributes = args[0];
        String pathFileWeatherDescription= args[1];

        //local mode
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");

        //cluster mode
/*        SparkConf conf = new SparkConf()
                .setAppName("Query 1")
                .set("spark.submit.deployMode","cluster");*/
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long iOperations = System.currentTimeMillis();

        //read file
        JavaRDD<String> file = sc.textFile(pathFileWeatherDescription);
        String header = file.first();
        String[] firstLine = header.split(",",-1);

        //get city names and timezones
        String[] cityNames;
        cityNames= Arrays.copyOfRange(firstLine, 1, firstLine.length);
        String[] finalCityNames = cityNames;
        GetterInfo getterInfo = new GetterInfo(sc, pathFileCityAttributes);
        long iTimeZone = System.currentTimeMillis();
        String[] timeZones = getterInfo.getTimeZoneFromCityName(sc, finalCityNames);
        long fTimeZone = System.currentTimeMillis();

        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> otherLines = file.filter(row -> !row.equals(header));
        JavaRDD<ArrayList<City>>  listOflistOfcities = otherLines
                .map(line -> CityParser.parseCSV(line, finalCityNames,timeZones));
        long fParseFile = System.currentTimeMillis();

        //convert to tuple5 City-Year-Month-Day-Weather_description
        //and take ongly desired month

        JavaRDD<Tuple5<String, Integer,Integer,Integer, String>> citiesParsed = listOflistOfcities
                .flatMap(new ParseRDDofLists())
                .filter(x-> desiredMonths.contains(x._3()));

        //check  number of hour in a day with sky is clear
        JavaPairRDD<Tuple4<String, Integer,Integer,Integer>, Integer> daySkyIsClear = citiesParsed
                .mapToPair(new ControlHour())
                .filter(x->x!=null)
                .reduceByKey((x,y) -> x+y)
                .filter(day->day._2>=minHoursIsClearForDay);

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
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to obtain TimeZones: %s ms\n", Long.toString(fTimeZone-iTimeZone));
        System.out.printf("Total time to parse %s: %s ms\n",pathFileWeatherDescription, Long.toString(fParseFile- iParseFile));
        System.out.printf("Total time after setting spark Spark Context: %s ms\n", Long.toString(finalTime - iOperations));
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime-initialTime));
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
