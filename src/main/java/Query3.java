import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.*;
import utils.City;
import utils.CityParser;
import utils.GetterInfo;

import java.lang.Double;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Query3 {

    private static final String pathToFile = "data/prj1_dataset/temperature.csv";
    private static final int desiredYear = 2017;
    private static final int[] desiredIntervalOfHours = {12,15};
    private static final int firstDesiredPosition = 3;
    private static List<Integer> desiredSummerMonths = new ArrayList<>(Arrays.asList(6,7,8,9));
    private static List<Integer> desiredWinterMonths = new ArrayList<>(Arrays.asList(1,2,3,4));



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
        String[] nations = getterInfo.getNationsFromCityName(finalCityNames);


        //get ther other lines of csv file
        JavaRDD<String> otherLines = file.filter(row -> !row.equals(header));
        JavaRDD<ArrayList<City>>  listOflistOfcities = otherLines
                .map(line -> CityParser.parseCSV(line, finalCityNames,timeZones, nations));

        //convert to tuple7 City-Nation-Year-Month-Day-Hour-Temperature
        JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> citiesParsed = listOflistOfcities
                .flatMap(new ParseRDDofLists())
                //take only possible temperature in Kelvin
                //and only desired interval of hours
                .filter(x->(x._7()>=173) && (x._7()<=373) &&
                        (x._6()>= desiredIntervalOfHours[0]) && (x._6()<= desiredIntervalOfHours[1]));


/*
        JavaPairRDD<Tuple3<String, String,Integer>,Double>averageOfSummerMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesParsed,desiredSummerMonths,desiredYear)
                        .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),x._4()));

        JavaPairRDD<Tuple3<String, String,Integer>,Double> averageOfWinterMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesParsed,desiredWinterMonths,desiredYear)
                        .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),x._4()));

        List<Tuple2<Double, Tuple3<String, String, Integer>>> topDiff = averageOfSummerMonths
                .join(averageOfWinterMonths)
                .mapToPair(x-> new Tuple2<>(x._2._1() - x._2._2(), new Tuple3<>(x._1._1(),x._1._2(),x._1._3()) ))
                .sortByKey(false)
                .take(firstDesiredPosition);
*/

        List<Tuple4<Double, String, String, Integer>> topOfDesiredYear =
                getTopOfYear(citiesParsed, desiredSummerMonths,desiredWinterMonths,desiredYear,firstDesiredPosition);

        List<Tuple4<Double, String, String, Integer>> topOfPreviusYear =
                getTopOfYear(citiesParsed,desiredSummerMonths,desiredWinterMonths,desiredYear-1,firstDesiredPosition);

        ArrayList<Integer> position = new ArrayList<>();

        for(int i=0; i<topOfDesiredYear.size(); i++){
            for(int j=0; j<topOfPreviusYear.size(); i++)
            if(topOfDesiredYear.get(i)._2().equalsIgnoreCase(topOfPreviusYear.get(j)._2()) &&
                    topOfDesiredYear.get(i)._3().equalsIgnoreCase(topOfPreviusYear.get(j)._3()))
                    position.add(j);

        }





       // List<Tuple4<String, String, Integer, Double>> print = averageOfSummerMonths .collect();
        //List<Tuple2<Tuple3<String, String, Integer>, Double>> print = averageOfWinterMonths .collect();
        //List<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> print = citiesParsed .collect();
        List<Tuple4<Double, String, String, Integer>> print1 = topOfDesiredYear;

        List<Tuple4<Double, String, String, Integer>> print2 = topOfPreviusYear;

        for(int i=0; i< print1.size(); i++){
            System.out.println("anno"+ print1.get(i));
            System.out.println("anno precedente"+ print2.get(i));
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

    private static  JavaRDD<Tuple4<String, String,Integer,Double>> getAverageOfDesiredMonthAndDesiredYear(
            JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> cities,
             List<Integer> months, int year){

        return cities
                .filter(x->( months.contains(x._4())))
                .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),new Tuple2<>(x._7() ,1)))
                .mapValues(x-> new Tuple2<>(x._1(),x._2()))
                .reduceByKey((x,y)-> (new Tuple2<>(x._1()+y._1(), x._2()+y._2())))
                .map(x-> new Tuple4<>(x._1._1(),x._1._2(),x._1._3(), (x._2._1/(x._2._2.doubleValue()))));
    }

    private static  List<Tuple4<Double, String, String, Integer>>getTopOfYear(
            JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> cities,
            List<Integer> summerMonths, List<Integer> winterMonths, int year, int numberOfposition
    ){
        JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> citiesOfYear = cities
                .filter(x->x._3().equals(year));

        JavaPairRDD<Tuple3<String, String,Integer>,Double>averageOfSummerMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesOfYear,summerMonths,year)
                        .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),x._4()));

        JavaPairRDD<Tuple3<String, String,Integer>,Double> averageOfWinterMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesOfYear,winterMonths,year)
                        .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),x._4()));

        List<Tuple4<Double, String, String, Integer>> topDiff = averageOfSummerMonths
                .join(averageOfWinterMonths)
                .mapToPair(x-> new Tuple2<>(x._2._1() - x._2._2(), new Tuple3<>(x._1._1(),x._1._2(),x._1._3()) ))
                .sortByKey(false)
                .map(x-> new Tuple4<>(x._1,x._2._2(),x._2._1(),x._2._3()))
                .take(numberOfposition);

        return topDiff;
    }
}
