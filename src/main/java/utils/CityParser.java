package utils;

import java.util.ArrayList;

public class CityParser {

    public static ArrayList<City> parseCSV(String csvLine, final String[] cityNames, final String[] cityTimeZones) {

        ArrayList<City> citiesOfALine = new ArrayList<>();
        String[] csvValues = csvLine.split(",", -1);


        for (int i=0; i< cityNames.length; i++){
            City city = new City();
            city.setCity(cityNames[i]);
            city.setTimestamp(cityTimeZones[i],csvValues[0]);
            city.setTimeZone(cityTimeZones[i]);
            city.setValue(csvValues[i+1]);

            if(!city.getValue().equals("") && !city.getTimestamp().equals(""))
                citiesOfALine.add(city);
        }

        return citiesOfALine;
    }

    public static ArrayList<City> parseCSV(String csvLine, String[] cityNames, String[] cityTimeZones, String[] nations) {
        ArrayList<City> citiesOfALine = new ArrayList<>();
        String[] csvValues = csvLine.split(",", -1);


        for (int i=0; i< cityNames.length; i++){
            City city = new City();
            city.setCity(cityNames[i]);
            city.setTimestamp(cityTimeZones[i],csvValues[0]);
            city.setTimeZone(cityTimeZones[i]);
            city.setValue(csvValues[i+1]);
            city.setNation(nations[i]);

            if(!city.getValue().equals("") && !city.getTimestamp().equals(""))
                citiesOfALine.add(city);
        }
        return citiesOfALine;
    }
}
