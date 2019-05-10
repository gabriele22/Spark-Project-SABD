package utils;

import java.util.ArrayList;

public class CityParser {

    public static ArrayList<City> parseCSV(String csvLine, final String[] cityNames, final String[] cityTimeZones) {

        ArrayList<City> citiesOfAline = new ArrayList<>();
        String[] csvValues = csvLine.split(",", -1);


        for (int i=0; i< cityNames.length; i++){
            City city = new City();
            city.setCity(cityNames[i]);
            city.setTimestamp(cityTimeZones[i],csvValues[0]);
            city.setTimeZone(cityTimeZones[i]);
            city.setWeather_condition(csvValues[i+1]);

            if(!city.getWeather_condition().equals("") && !city.getTimestamp().equals(""))
                citiesOfAline.add(city);
        }




        return citiesOfAline;
    }
}
