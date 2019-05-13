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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Query3 {

    private static final String pathToFile = "data/prj1_dataset/temperature.csv";
    private static final int[] desiredYear = {2017,2016};
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
        String[] nations = getterInfo.getNationsFromCityName(finalCityNames, sc);
        List<String> distinctNations = Arrays.stream(nations).distinct().collect(Collectors.toList());

/*        distinctNations.forEach(x->
            System.out.println(x));*/



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




        for(String nation: distinctNations) {
/*            List<Tuple4<Double, String, String, Integer>> topOfDesiredYear =
                    getTopOfYear(citiesParsed, desiredSummerMonths, desiredWinterMonths,
                            desiredYear[0], firstDesiredPosition,nation);

            List<Tuple4<Double, String, String, Integer>> topOfPreviusYear =
                    getTopOfYear(citiesParsed, desiredSummerMonths, desiredWinterMonths,
                            desiredYear[1], firstDesiredPosition, nation);

            List<Tuple4<Double, String, String, Integer>> print1 = topOfDesiredYear;

            List<Tuple4<Double, String, String, Integer>> print2 = topOfPreviusYear;*/
            List<Tuple2<Double, String>> topOfDesiredYear =
                    getTopOfYear(citiesParsed, desiredSummerMonths, desiredWinterMonths,
                            desiredYear[0], firstDesiredPosition,nation);

            List<Tuple2<Double, String>> topOfPreviusYear =
                    getTopOfYear(citiesParsed, desiredSummerMonths, desiredWinterMonths,
                            desiredYear[1], cityNames.length, nation);

            List<Tuple2<Double, String>> print1 = topOfDesiredYear;

            List<Tuple2<Double, String>> print2 = topOfPreviusYear;



            ArrayList<Integer> positions = new ArrayList<>();

            for (int i = 0; i < topOfDesiredYear.size(); i++) {
                for (int j = 0; j < topOfPreviusYear.size(); j++)
                    if(topOfPreviusYear.get(j)._2().equals(topOfDesiredYear.get(i)._2()))
                        positions.add(j+1);

            }
            for(int i=0; i< print1.size(); i++){
                System.out.println(print1.get(i)+ "posizione anno precedente"+ positions.get(i));
                //System.out.println("anno precedente"+ print2.get(i));
            }
        }





       // List<Tuple4<String, String, Integer, Double>> print = averageOfSummerMonths .collect();
        //List<Tuple2<Tuple3<String, String, Integer>, Double>> print = averageOfWinterMonths .collect();
        //List<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> print = citiesParsed .collect();




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

    private static  List<Tuple2<Double, String>>getTopOfYear(
            JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> cities,
            List<Integer> summerMonths, List<Integer> winterMonths, int year, int numberOfposition, String nation
    ){
        JavaRDD<Tuple7<String,String, Integer,Integer,Integer,Integer, Double>> citiesOfYearNation = cities
                .filter(x->x._3().equals(year) && x._2().equals(nation));

        //TODO  come ritorno mesi estivi(e invernali) far ritornare solo String=città e Double = media

/*
        JavaPairRDD<Tuple3<String, String,Integer>,Double>averageOfSummerMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesOfYearNation,summerMonths,year)
                        .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),x._4()));

        JavaPairRDD<Tuple3<String, String,Integer>,Double> averageOfWinterMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesOfYearNation,winterMonths,year)
                        .mapToPair(x-> new Tuple2<>(new Tuple3<>(x._1(),x._2(),x._3()),x._4()));

        List<Tuple4<Double, String, String, Integer>> topDiff = averageOfSummerMonths
                .join(averageOfWinterMonths)
                .mapToPair(x-> new Tuple2<>(x._2._1() - x._2._2(), new Tuple3<>(x._1._1(),x._1._2(),x._1._3()) ))
                .sortByKey(false)
                .map(x-> new Tuple4<>(x._1,x._2._2(),x._2._1(),x._2._3()))
                .take(numberOfposition);
*/
        JavaPairRDD<String, Double>averageOfSummerMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesOfYearNation,summerMonths,year)
                        .mapToPair(x-> new Tuple2<>(x._1(),x._4()));

        JavaPairRDD<String, Double>averageOfWinterMonths =
                getAverageOfDesiredMonthAndDesiredYear(citiesOfYearNation,winterMonths,year)
                        .mapToPair(x-> new Tuple2<>(x._1(),x._4()));

        List<Tuple2<Double, String>> topDiff = averageOfSummerMonths
                .join(averageOfWinterMonths)
                .mapToPair(x-> new Tuple2<>(x._2._1() - x._2._2(), x._1()))
                .sortByKey(false)
                .map(x-> new Tuple2<>(x._1(),x._2()))
                .take(numberOfposition);



        return topDiff;
    }

    private static class mapIterator {
    }
}
