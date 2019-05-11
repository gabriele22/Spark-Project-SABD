import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple5;
import scala.Tuple6;
import utils.City;
import utils.CityParser;
import utils.GetterInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Query3 {

    private static String pathToFile = "data/prj1_dataset/temperature.csv";

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

        //convert to tuple6 City-Nation-Year-Month-Day-Temperature
        JavaRDD<Tuple6<String,String, Integer,Integer,Integer, Double>> citiesParsed = listOflistOfcities
                .flatMap(new ParseRDDofLists())
                //add only city with possible temperature in Kelvin
                //considering approximately the lowest value and the highest value ever recorded (in celsius -100°C<t<50°C)
                .filter(x->(x._6()>=173) && (x._6()<=323));

        List<Tuple6<String, String, Integer, Integer, Integer, Double>> print = citiesParsed.collect();

        for(int i=0; i< print.size(); i++){
            System.out.println(print.get(i));
        }


        sc.stop();
    }
//add only city with possible temperature in Kelvin
                //considering approximately the lowest value and the highest value ever recorded (in celsius -100°C<t<50°C)


    private static class ParseRDDofLists implements FlatMapFunction<ArrayList<City>, Tuple6<String,String, Integer,Integer,Integer, Double> > {
        @Override
        public Iterator<Tuple6<String,String, Integer, Integer, Integer, Double>> call(ArrayList<City> cities) throws Exception {
            List<Tuple6<String,String, Integer, Integer, Integer, Double>> results= new ArrayList<>();
            for (City city : cities) {
                double temperature = Double.parseDouble(city.getValue());
                Tuple6<String, String, Integer, Integer, Integer, Double> result = new Tuple6<>(city.getCity(), city.getNation(),
                            city.getYear(), city.getMonth(), city.getDay(), temperature);
                results.add(result);

            }
            return results.iterator();
        }
    }
}
