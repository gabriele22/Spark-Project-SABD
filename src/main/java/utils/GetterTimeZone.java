package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class GetterTimeZone {
    private ArrayList<CityCoordinate> cityCoordinatesArrayList;
    private static String fileTimeZone = "data/timezone.csv";
    private  static String fileCityAttributes = "data/prj1_dataset/city_attributes.csv";

    public GetterTimeZone() {
        this.cityCoordinatesArrayList = getCoordinates(fileCityAttributes);
    }



    public String getTimeZone (String cityName){
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

    public String[] getTimeZoneFromCityName(String[] cityNames) {
        String[] timeZones = new String[cityNames.length];
        for (int i=0; i<cityNames.length; i++){
            GetterTimeZone getterTimeZone = new GetterTimeZone();
            timeZones[i] = getterTimeZone.getTimeZone(cityNames[i]);
            // System.out.println("timezone: "+timeZones[i]+ " city: "+ finalCityNames[i]);
        }
        return timeZones;
    }

}
