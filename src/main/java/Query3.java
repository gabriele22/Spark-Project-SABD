import com.clearspring.analytics.util.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.*;
import utils.City;
import utils.CityParser;
import utils.GetterInfo;
import java.lang.Double;
import java.lang.Long;
import java.util.*;
import java.util.stream.Collectors;

/**
 this class (by default) finds, for each nation,
 the three cities that have registered in 2017 the maximum difference in average temperatures
 in the local time slot 12: 00-15: 00 in June, July, August and September
 compared to the months of January, February, March and April.
 It also compares the position of cities in the ranking of the previous year
 */
public class Query3 {
    private static final int[] desiredIntervalOfHours = {12,15};
    private static List<Integer> desiredMonths = new ArrayList<>(Arrays.asList(1,2,3,4,6,7,8,9));
    private static List<Integer> desiredSummerMonths = new ArrayList<>(Arrays.asList(6,7,8,9));
    private static List<Integer> desiredWinterMonths = new ArrayList<>(Arrays.asList(1,2,3,4));
    private static List<Integer> desiredYearList = new ArrayList<>(Arrays.asList(2017,2016));

    private static final int firstDesiredPosition = 3;


    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("\nERROR: Insert arguments in this order: " +
                    "1. 'file city-attributes, 2. file temperatures 3. redis ip'");
        }

        long initialTime = System.currentTimeMillis();

        String pathFileCityAttributes = args[0] ;
        String pathFileTemperature= args[1];

        //local mode
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 3");

        //cluster mode
/*        SparkConf conf = new SparkConf()
                .setAppName("Query 3")
                .set("spark.submit.deployMode","cluster");*/
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long iOperations = System.currentTimeMillis();

        //read file
        JavaRDD<String> file= sc.textFile(pathFileTemperature);
        String header = file.first();
        String[] firstLine = header.split(",",-1);

        //get city names and timezones
        String[] cityNames;
        cityNames= Arrays.copyOfRange(firstLine, 1, firstLine.length);
        boolean useRedis = true;
        List<String> distinctCities = Arrays.stream(cityNames).distinct().collect(Collectors.toList());
        if(cityNames.length != distinctCities.size()) {useRedis=false;}
        String[] finalCityNames = cityNames;
        GetterInfo getterInfo = new GetterInfo(sc,pathFileCityAttributes);
        long iTimeZone = System.currentTimeMillis();
        String[] timeZones = getterInfo.getTimeZoneFromCityName( sc,finalCityNames);
        long fTimeZone = System.currentTimeMillis();
        //get nations
        long iNations = System.currentTimeMillis();
        String[] nations = getterInfo.getNationsFromCityName(args[2],finalCityNames, useRedis);
        long fNations = System.currentTimeMillis();
        List<String> distinctNations = Arrays.stream(nations).distinct().collect(Collectors.toList());

        //get ther other lines of csv file
        long iParseFile = System.currentTimeMillis();
        JavaRDD<String> otherLines = file.filter(row -> !row.equals(header));
        JavaRDD<ArrayList<City>>  listOflistOfcities = otherLines
                .map(line -> CityParser.parseCSV(line, finalCityNames,timeZones, nations));
        long fParseFile = System.currentTimeMillis();


        //convert to tuple7 City-Nation-Year-Month-Day-Hour-Temperature
        // and take only useful tuples
        JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> citiesParsed = listOflistOfcities
                .flatMap(new ParseRDDofLists())
                .filter(x->(desiredYearList.contains(x._3()) && (desiredMonths.contains(x._4()))))
                .filter(x->((x._6()>= desiredIntervalOfHours[0]) && (x._6()<= desiredIntervalOfHours[1])))
                .filter(x->(x._7()>=173.15) && (x._7()<=373.15));

        //get difference between summer and winter months,
        //sorting by difference
        // and grouping by nation and year
        JavaPairRDD<Tuple2<Integer, String>, Iterable<Tuple2<String,Double>>> topDiff = citiesParsed
                .mapToPair(new mapToAverage())
                .reduceByKey((x, y) -> new Tuple4<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3(), x._4() + y._4()))
                .mapToPair(x -> new Tuple2<>(x._2._1() / x._2._2().doubleValue() - x._2._3() / x._2._4().doubleValue(),
                        new Tuple3<>(x._1._1(), x._1._2(), x._1._3())))
                .sortByKey(false)
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._2._3(), x._2._2()), new Tuple2<>(x._2._1(), x._1)))
                .groupByKey()
                .cache();

        //get the rankings for the two years for each nation
        //and get positions in the previous year
        for(String nation: distinctNations) {
            List<Tuple2<Tuple2<Integer, String>, Iterable<Tuple2<String, Double>>>> topDesiredYear = topDiff
                    .filter(x -> x._1()._2().equals(nation) && x._1()._1().equals(desiredYearList.get(0)))
                    .collect();

            List<Tuple2<Tuple2<Integer, String>, Iterable<Tuple2<String, Double>>>> topPreviousYear = topDiff
                    .filter(x -> x._1()._2().equals(nation) && x._1()._1().equals(desiredYearList.get(1)))
                    .collect();

            List<Tuple2<String,Double>> ranking = Lists.newArrayList(topDesiredYear.get(0)._2());
            List<Tuple2<String,Double>> rankingPrev = Lists.newArrayList(topPreviousYear.get(0)._2());

            ArrayList<Integer> positions = new ArrayList<>();
            System.out.println("\nNation: "+ nation + "   Ranking Year "+ desiredYearList.get(0));

            //Print in tabular format
            System.out.println("---------------------------------------------------------");
            System.out.printf("%4s %15s %10s %10s %s ", "RANK", "CITY", "VALUE", "YEAR", desiredYearList.get(1));
            System.out.println();
            System.out.println("---------------------------------------------------------");
            for(int i = 0; i < firstDesiredPosition; i++){
                for (int j = 0; j < rankingPrev.size(); j++) {
                    if (rankingPrev.get(j)._1().equals(ranking.get(i)._1()))
                        positions.add(j + 1);
                }
                int rank = i+1;
                System.out.format("%4d %15s %10.2f %10d ",
                        rank, ranking.get(i)._1(), ranking.get(i)._2(), positions.get(i));
                System.out.println();
            }
            System.out.println("---------------------------------------------------------\n");


        }
        long fOperations = System.currentTimeMillis();


        System.out.printf("Total time to obtain TimeZones: %s ms\n", Long.toString(fTimeZone - iTimeZone));
        System.out.printf("Total time to obtain Nations: %s ms\n", Long.toString(fNations - iNations));
        System.out.printf("Total time to parse %s: %s ms\n",pathFileTemperature, Long.toString(fParseFile- iParseFile));
        System.out.printf("Total time to execute operations: %s ms\n", Long.toString(fOperations - iOperations));
        sc.stop();
        long finalTime = System.currentTimeMillis();
        System.out.printf("Total time to complete: %s ms\n", Long.toString(finalTime-initialTime));
    }


    private static class ParseRDDofLists implements FlatMapFunction<ArrayList<City>, Tuple7<String,String, Integer,Integer,Integer,Integer, Double> > {
        @Override
        public Iterator<Tuple7<String,String,Integer, Integer, Integer, Integer, Double>> call(ArrayList<City> cities) {
            List<Tuple7<String,String,Integer, Integer, Integer, Integer, Double>> results= new ArrayList<>();
            for (City city : cities) {
                Tuple7<String, String, Integer, Integer, Integer,Integer, Double> result =
                        new Tuple7<>(city.getCity(), city.getNation(),
                                city.getYear(), city.getMonth(), city.getDay(),city.getHour(), city.getTemperature());
                results.add(result);

            }
            return results.iterator();
        }
    }


    private static class mapToAverage implements PairFunction<Tuple7<String, String, Integer, Integer, Integer, Integer, Double>,
            Tuple3<String,String,Integer>, Tuple4<Double,Integer,Double,Integer>> {
        @Override
        public Tuple2<Tuple3<String, String, Integer>, Tuple4<Double, Integer, Double, Integer>> call(
                Tuple7<String, String, Integer, Integer, Integer, Integer, Double> stringStringIntegerIntegerIntegerIntegerDoubleTuple7) {

            Tuple7<String,String, Integer, Integer, Integer, Integer, Double> x = stringStringIntegerIntegerIntegerIntegerDoubleTuple7;
            Tuple2<Tuple3<String, String, Integer>, Tuple4<Double, Integer, Double, Integer>> pair;
            Tuple2<Double,Integer> sumCountSummer = null;
            Tuple2<Double,Integer> sumCountWinter = null;

            if(desiredSummerMonths.contains(x._4())) {
                sumCountSummer = new Tuple2<>(x._7(), 1);
                sumCountWinter = new Tuple2<>(0.0,0);
            }


            if(desiredWinterMonths.contains(x._4())) {
                sumCountWinter = new Tuple2<>(x._7(), 1);
                sumCountSummer = new Tuple2<>(0.0,0);
            }

            pair = new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),
                    new Tuple4<>(sumCountSummer._1(),sumCountSummer._2(),sumCountWinter._1(),sumCountWinter._2()));

            return pair;
        }
    }
}