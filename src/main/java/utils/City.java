package utils;


import org.joda.time.DateTime;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.time.ZoneOffset.UTC;

public class City implements Serializable {

    private String city;
    private String timestamp;
    private String weather_condition;
    private String timeZone;

    public City() {
    }

    public City(String city, String timestamp, String weather_condition) {
        this.city = city;
        this.timestamp = timestamp;
        this.weather_condition = weather_condition;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getWeather_condition() {
        return weather_condition;
    }

    public void setWeather_condition(String weather_condition) {
        this.weather_condition = weather_condition;
    }


    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

        public int getYear () {
            return getFormatter().getYear();
        }

    public int getMonth () {
        return getFormatter().getMonthOfYear();

    }

    public int getDay () {
        return getFormatter().getDayOfMonth();

    }

    private String fromUTCToLocal(String idTimeZone, String timeStamp){
        String localDate;
        Date dateTimeInUTC = null;
        DateFormat timeUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        timeUTC.setTimeZone(TimeZone.getTimeZone(UTC));

        try {
            dateTimeInUTC = timeUTC.parse(timeStamp);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        SimpleDateFormat localdateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
        localdateFormat.setTimeZone(TimeZone.getTimeZone(idTimeZone));
        localDate = localdateFormat.format(dateTimeInUTC);
        //System.out.println("dal csv: "+ timeStamp+ "local: "+ localDate);
        return localDate;
    }

    private DateTime getFormatter(){
        //DateTimeFormatter format= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat localdateFormat = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
        Date date=null;
        try {
            date = localdateFormat.parse(this.getTimestamp());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return new DateTime(date);
    }



/*    public int getYear(){
        DateTimeFormatter format= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        DateTime date=DateTime.parse(this.getTimestamp(), format);
        return date.getYear();
    }


    public int getMonth () {
        DateTimeFormatter format= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime date=DateTime.parse(this.getTimestamp(), format);
        return date.getMonthOfYear();

    }


    public int getDay () {
        DateTimeFormatter format= DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime date=DateTime.parse(this.getTimestamp(), format);
        return date.getDayOfMonth();
    }*/






    public void setTimestamp(String timeZone,String timestamp) {

        this.timestamp=fromUTCToLocal(timeZone,timestamp);

    }

    public String getTimestamp() {
        return timestamp;
    }
}

