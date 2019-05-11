package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.UnknownHostException;
import java.util.ArrayList;

import static io.restassured.RestAssured.get;


public class GetterInfo {
    private ArrayList<CityCoordinate> cityCoordinatesArrayList;
    private static final String fileTimeZone = "data/timezone.csv";
    private static final String fileCityAttributes = "data/prj1_dataset/city_attributes.csv";
    private static final String fileNationCities = "data/worldCities.csv";

    //when the class is instantiated the file with the city coordinates is read
    public GetterInfo() {
        this.cityCoordinatesArrayList = getCoordinates(fileCityAttributes);
    }


    //return the nearest timezone based on the coordinate
    private String getTimeZone(String cityName){
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ";";

        CityCoordinate cityCoordinate = new CityCoordinate();
        String timezone= null;
        double distance = 100000000;
   /*     Double latMin = null;
        Double longMin = null;*/

        for (CityCoordinate coordinate : cityCoordinatesArrayList) {
            if (coordinate.getCity().equals(cityName)) {
                cityCoordinate.setCity(cityName);
                cityCoordinate.setLat(coordinate.getLat());
                cityCoordinate.setLong(coordinate.getLong());
            }
        }

        try {
            br = new BufferedReader(new FileReader(fileTimeZone));

            while ((line = br.readLine()) != null) {
                String[] values = line.split(cvsSplitBy, -1);

                double diffLat = Math.abs(Double.parseDouble(cityCoordinate.getLat())- Double.parseDouble(values[1]));
                double diffLong = Math.abs(Double.parseDouble(cityCoordinate.getLong()) - Double.parseDouble(values[2]));
                double currentDistance = Math.hypot(diffLat,diffLong);

                if(currentDistance<distance) {
                    timezone = values[0];
                    distance=currentDistance;
                }


            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return  timezone;
    }


    private ArrayList<CityCoordinate> getCoordinates(String csvFile) {

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        cityCoordinatesArrayList = new ArrayList<>();

        try {

            br = new BufferedReader(new FileReader(csvFile));
            int index=0;
            while ((line = br.readLine()) != null) {

                if(index>0) {//skip firs line
                    String[] values = line.split(cvsSplitBy, -1);

                    CityCoordinate city = new CityCoordinate();
                    city.setCity(values[0]);
                    city.setLat(values[1]);
                    city.setLong(values[2]);

                    cityCoordinatesArrayList.add(city);
                }
                index++;

            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return cityCoordinatesArrayList;
    }


    //first try to find nation in the file word_cities.csv
    //if the city is not present in the file it obtains the nation by making a request to the site www.geonames.org
    private String getNation(String cityName) {
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        String nation = "";

        try {
            br = new BufferedReader(new FileReader(fileNationCities));
            int index=0;

            while ((line = br.readLine()) != null) {

                String[] values = line.split(cvsSplitBy, -1);
                if(index>0) {//skip firs line
                    if(values[0].contains(cityName)){
                        nation= nation + values[1];
                        return nation;
                    }
                }
                index++;
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
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
            }catch ( Exception ue){
                System.err.println("\nControl your Internet Connection");
                System.exit(1);
            }
        }
        return nation;
    }


    public String[] getTimeZoneFromCityName(String[] cityNames) {
        String[] timeZones = new String[cityNames.length];
        for (int i=0; i<cityNames.length; i++){
            GetterInfo getterInfo = new GetterInfo();
            timeZones[i] = getterInfo.getTimeZone(cityNames[i]);
        }
        return timeZones;
    }

    public String[] getNationsFromCityName(String[] cityNames) {
        String[] nations = new String[cityNames.length];
        for (int i=0; i<cityNames.length; i++){
            GetterInfo getterInfo = new GetterInfo();
            nations[i] = getterInfo.getNation(cityNames[i]);
        }
        return nations;
    }
}
