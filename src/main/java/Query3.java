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
import java.util.*;
import java.util.stream.Collectors;

public class Query3 {

    private static final String pathToFile = "data/prj1_dataset/temperature.csv";
    private static final int[] desiredIntervalOfHours = {12,15};
    private static List<Integer> desiredMonths = new ArrayList<>(Arrays.asList(1,2,3,4,6,7,8,9));
    private static List<Integer> desiredSummerMonths = new ArrayList<>(Arrays.asList(6,7,8,9));
    private static List<Integer> desiredWinterMonths = new ArrayList<>(Arrays.asList(1,2,3,4));
    private static List<Integer> desiredYearList = new ArrayList<>(Arrays.asList(2017,2016));

    private static final int firstDesiredPosition = 3;


    public static void main(String[] args) {

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
        String[] nations = getterInfo.getNationsFromCityName(finalCityNames, sc);
        List<String> distinctNations = Arrays.stream(nations).distinct().collect(Collectors.toList());


        //get ther other lines of csv file
        JavaRDD<String> otherLines = file.filter(row -> !row.equals(header));
        JavaRDD<ArrayList<City>>  listOflistOfcities = otherLines
                .map(line -> CityParser.parseCSV(line, finalCityNames,timeZones, nations));

        //convert to tuple7 City-Nation-Year-Month-Day-Hour-Temperature
        JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> citiesParsed = listOflistOfcities
                .flatMap(new ParseRDDofLists())
                .filter(x->(x._7()>=173.15) && (x._7()<=373.15)
                        && (x._6()>= desiredIntervalOfHours[0]) && (x._6()<= desiredIntervalOfHours[1])
                        && desiredYearList.contains(x._3())
                        && desiredMonths.contains(x._4()));


        JavaPairRDD<Tuple2<Integer, String>, Iterable<Tuple2<String,Double>>> topDiff = citiesParsed
                .mapToPair(new mapToAverage())
                .reduceByKey((x, y) -> new Tuple4<>(x._1() + y._1(), x._2() + y._2(), x._3() + y._3(), x._4() + y._4()))
                .mapToPair(x -> new Tuple2<>(x._2._1() / x._2._2().doubleValue() - x._2._3() / x._2._4().doubleValue(),
                        new Tuple3<>(x._1._1(), x._1._2(), x._1._3())))
                .sortByKey(false)
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._2._3(), x._2._2()), new Tuple2<>(x._2._1(), x._1)))
                .groupByKey();


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
            System.out.println("Nation: "+ nation + "   Ranking Year "+ desiredYearList.get(0));
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


        sc.stop();
    }


    private static class ParseRDDofLists implements FlatMapFunction<ArrayList<City>, Tuple7<String,String, Integer,Integer,Integer,Integer, Double> > {
        @Override
        public Iterator<Tuple7<String,String,Integer, Integer, Integer, Integer, Double>> call(ArrayList<City> cities) throws Exception {
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
        public Tuple2<Tuple3<String, String, Integer>, Tuple4<Double, Integer, Double, Integer>> call(Tuple7<String, String, Integer, Integer, Integer, Integer, Double> stringStringIntegerIntegerIntegerIntegerDoubleTuple7) throws Exception {
            Tuple7<String,String, Integer, Integer, Integer, Integer, Double> x = stringStringIntegerIntegerIntegerIntegerDoubleTuple7;
            Tuple2<Tuple3<String, String, Integer>, Tuple4<Double, Integer, Double, Integer>> pair = null;
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
