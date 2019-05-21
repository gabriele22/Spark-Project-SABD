package utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import static io.restassured.RestAssured.*;

public class GetterInfo {
    private ArrayList<CityCoordinate> cityCoordinatesArrayList;
    private static final String fileTimeZone = "data/timeZone.csv";
    private static final String fileNationCities = "data/worldCities.csv";

    //file with the city coordinates is read only one time
    public GetterInfo(JavaSparkContext sc, String fileCityAttributes) {
        this.cityCoordinatesArrayList = getCoordinates(fileCityAttributes, sc);
    }


    //return the nearest timezone based on the coordinate
    private String getTimeZone(String cityName, JavaSparkContext sc){


        CityCoordinate cityCoordinate = new CityCoordinate();
        String timezone= null;
        double distance = 100000000;

        for (CityCoordinate coordinate : cityCoordinatesArrayList) {
            if (coordinate.getCity().equals(cityName)) {
                cityCoordinate.setCity(cityName);
                cityCoordinate.setLat(coordinate.getLat());
                cityCoordinate.setLong(coordinate.getLong());
            }
        }

        JavaRDD<String> file = sc.textFile(fileTimeZone);
        List<String> lines = file
                .collect();

        for(String line: lines) {
            String[] values = line.split(";", -1);

            double diffLat = Math.abs(Double.parseDouble(cityCoordinate.getLat())- Double.parseDouble(values[1]));
            double diffLong = Math.abs(Double.parseDouble(cityCoordinate.getLong()) - Double.parseDouble(values[2]));
            double currentDistance = Math.hypot(diffLat,diffLong);

            if(currentDistance<distance) {
                timezone = values[0];
                distance=currentDistance;
            }


        }

        return  timezone;
    }


    private ArrayList<CityCoordinate> getCoordinates(String csvFile, JavaSparkContext sc) {
        cityCoordinatesArrayList = new ArrayList<>();

        JavaRDD<String> file = sc.textFile(csvFile);
        String header = file.first();
        //get ther other lines of csv file
        List<String> otherLines = file
                .filter(row -> !row.equals(header))
                .collect();
        for(String line: otherLines) {
            String[] values = line.split(",", -1);

            CityCoordinate city = new CityCoordinate();
            city.setCity(values[0]);
            city.setLat(values[1]);
            city.setLong(values[2]);

            cityCoordinatesArrayList.add(city);

        }

        return cityCoordinatesArrayList;
    }


    //first try to find nation in the file word_cities.csv
    //if the city is not present in the file it obtains the nation by making a request to the site www.geonames.org
/*    private String getNation(String cityName, JavaSparkContext sc) {
        String nation = "";
        JavaRDD<String> file = sc.textFile(fileNationCities);
        String header = file.first();
        //get ther other lines of csv file
        List<String> otherLines = file
                .filter(row -> !row.equals(header))
                .collect();
        for(String line: otherLines) {
            String[] values = line.split(",", -1);
            if(values[0].contains(cityName)){
                nation= nation + values[1];
                return nation;
            }
        }

        if(nation.length()==0){
            CityCoordinate cityCoordinate = new CityCoordinate();

            for (CityCoordinate coordinate : cityCoordinatesArrayList) {
                if (coordinate.getCity().equals(cityName)) {
                    cityCoordinate.setCity(cityName);
                    cityCoordinate.setLat(coordinate.getLat());
                    cityCoordinate.setLong(coordinate.getLong());
                }
            }
            try {
                nation = get("http://www.geonames.org/findNearbyPlaceName?lat={lat}&lng={long}",
                        cityCoordinate.getLat(), cityCoordinate.getLong())
                        .xmlPath().getString("geonames.geoname.countryName");
            }catch ( Exception e){
                System.err.println("\nControl your Internet Connection");
                System.exit(1);
            }
        }
        return nation;
    }*/

    private String getNation(String cityName, String redisIP) {
        //Connecting to Redis server
        Jedis jedis = new Jedis(redisIP);
        String nation = jedis.get(cityName);

        if(nation==null){
            System.out.println(cityName);
            CityCoordinate cityCoordinate = new CityCoordinate();

            for (CityCoordinate coordinate : cityCoordinatesArrayList) {
                if (coordinate.getCity().equals(cityName)) {
                    cityCoordinate.setCity(cityName);
                    cityCoordinate.setLat(coordinate.getLat());
                    cityCoordinate.setLong(coordinate.getLong());
                }
            }
            try {
                nation = get("http://www.geonames.org/findNearbyPlaceName?lat={lat}&lng={long}",
                        cityCoordinate.getLat(), cityCoordinate.getLong())
                        .xmlPath().getString("geonames.geoname.countryName");
            }catch ( Exception e){
                System.err.println("\nControl your Internet Connection");
                System.exit(1);
            }
        }
        return nation;
    }




    public String[] getTimeZoneFromCityName(JavaSparkContext sc, String[] cityNames) {
        String[] timeZones = new String[cityNames.length];
        for (int i=0; i<cityNames.length; i++){
            timeZones[i] = getTimeZone(cityNames[i], sc);
        }
        return timeZones;
    }

    public String[] getNationsFromCityName(String redisIp, String[] cityNames) {
        String[] nations = new String[cityNames.length];
        for (int i=0; i<cityNames.length; i++){
            //nations[i] = getNation(cityNames[i], sc);
            nations[i] = getNation(cityNames[i], redisIp);
        }
        return nations;
    }
}
