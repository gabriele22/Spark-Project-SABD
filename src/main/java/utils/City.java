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
    private String value;
    private String timeZone;
    private String nation;

    public City() {
    }

    public City(String city, String timestamp, String value) {
        this.city = city;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getNation() {
        return nation;
    }

    public void setNation(String nation) {
        this.nation = nation;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
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

    public int getMonth () { return getFormatter().getMonthOfYear(); }

    public int getDay () {return getFormatter().getDayOfMonth();}

    public int getHour(){return getFormatter().getHourOfDay();}


    public void setTimestamp(String timeZone,String timestamp) {

        this.timestamp=fromUTCToLocal(timeZone,timestamp);

    }

    public String getTimestamp() {
        return timestamp;
    }

    public Double getTemperature(){
        return Double.parseDouble(this.getValue());
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

}

