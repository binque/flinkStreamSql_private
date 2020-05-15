/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 

package com.cj.flink.sql.util;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;


/**
 * 
 * Reason: TODO ADD REASON(可选)
 * Date: 2017年03月10日 下午1:16:37
 * Company: www.dtstack.com
 * @author sishu.yss
 *
 */
public class DateUtil {

    static final String timeZone = "GMT+8";
    static final String datetimeFormat = "yyyy-MM-dd HH:mm:ss";
    static final String dateFormat = "yyyy-MM-dd";
    static final String timeFormat = "HH:mm:ss";
    static final SimpleDateFormat datetimeFormatter = new SimpleDateFormat(datetimeFormat);
    static final SimpleDateFormat dateFormatter = new SimpleDateFormat(dateFormat);
    static final SimpleDateFormat timeFormatter = new SimpleDateFormat(timeFormat);

    private static final Pattern DATETIME = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3,9})?Z$");
    private static final Pattern DATE = Pattern.compile("^\\d{4}-(?:0[0-9]|1[0-2])-[0-9]{2}$");

    private static final int MILLIS_PER_SECOND = 1000;


    public static java.sql.Date columnToDate(Object column) {
        if(column instanceof String) {
            return new java.sql.Date(stringToDate((String)column).getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new java.sql.Date(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (java.sql.Date) column;
        } else if(column instanceof Timestamp) {
            Timestamp ts = (Timestamp) column;
            return new java.sql.Date(ts.getTime());
        }
        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static Date stringToDate(String strDate)  {
        if(strDate == null){
            return null;
        }
        try {
            return datetimeFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return dateFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        try {
            return timeFormatter.parse(strDate);
        } catch (ParseException ignored) {
        }

        throw new RuntimeException("can't parse date");
    }

    /**
     *
     * 
     * @param day Long 时间
     * @return long
     */
    public static long getTodayStart(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000));
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     *
     * @param day Long 时间
     * @param scope
     * @return
     */
    public static long getTodayStart(long day,String scope) {
    	if(scope.equals("MS")){
    		return getTodayStart(day)*1000;
    	}else if(scope.equals("S")){
    		return getTodayStart(day);
    	}else{
    		return getTodayStart(day);
    	}
    }

    /**
     *
     * @param day Long 时间
     * @return long
     */
    public static long getNextDayStart(long day) {
        long daySpanMill = 86400000L;
        long nextDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000));
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        nextDay = (cal.getTimeInMillis() + daySpanMill) / 1000;
        return nextDay;
    }
    
    /**
     *
     * @param day Long 时间
     * @param scope String 级别<br>"MS"：毫秒级<br>"S":秒级
     * @return
     */
    public static long getNextDayStart(long day,String scope) {
    	if(scope.equals("MS")){
    		return getNextDayStart(day)*1000;
    	}else if(scope.equals("S")){
    		return getNextDayStart(day);
    	}else{
    		return getNextDayStart(day);
    	}
    }
    

    /**
     *
     * @param day
     * @return
     */
    public static long getMonthFirst(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     * @param day
     * @return
     */
    public static int getMonth(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.MONTH) + 1;
    }

    /**
     *
     * @author yumo.lck
     */
    public static int getYear(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.YEAR);
    }

    /**
     *
     * @param day
     * @return
     */
    public static long getWeekFirst(long day) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     * 根据某个日期时间戳秒值，获取所在周在一年中是第几周.
     * 
     * @param day
     * @return
     */
    public static int getWeekOfYear(long day) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        return cal.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String getYesterdayByString(String day, String inFormat, String outFormat){
        try {
			SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
			Date date = sdf.parse(day);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int calendarDay = calendar.get(Calendar.DATE);
			calendar.set(Calendar.DATE, calendarDay - 1);
			String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
			return dayBefore;
		} catch (ParseException e) {
			return null;
		}
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String getTomorrowByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 1);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }
    
    /**
     *
     * @param date
     * @return Date
     * @throws ParseException
     */
    public static Date getTomorrowByDate(Date date) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay + 1);
        return calendar.getTime();
    }

    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String get30DaysBeforeByString(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int calendarDay = calendar.get(Calendar.DATE);
        calendar.set(Calendar.DATE, calendarDay - 30);
        return new SimpleDateFormat(outFormat).format(calendar.getTime());
    }
    
    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String get30DaysLaterByString(String day, String inFormat, String outFormat) throws ParseException {
    	SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
    	Date date = sdf.parse(day);
    	Calendar calendar = Calendar.getInstance();
    	calendar.setTime(date);
    	int calendarDay = calendar.get(Calendar.DATE);
    	calendar.set(Calendar.DATE, calendarDay + 30);
    	String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
    	return dayBefore;
    }


    /**
     *
     * @param day
     * @param inFormat
     * @param outFormat
     * @return String
     * @throws ParseException
     */
    public static String getDateStrTOFormat(String day, String inFormat, String outFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        String dayBefore = new SimpleDateFormat(outFormat).format(calendar.getTime());
        return dayBefore;
    }
    
    public static long getDateMillTOFormat(String day, String inFormat) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(inFormat);
        Date date = sdf.parse(day);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.getTimeInMillis()/1000;
    }

    /**
     *
     * @author sishu.yss
     * @param year
     * @param month
     * @return
     */
    public static long getFirstDay4Month(int year, int month) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     *
     * @author yumo.lck
     * @param year
     * @param month
     * @return
     */
    public static long getLastDay4Month(int year, int month) {
        long lastDay = 0L;
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month);
        //1 represents a zero next month, can be seen as the end of the first day of the month most one day, but the data table on the last day of the zero point on the line
        cal.set(Calendar.DAY_OF_MONTH, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        lastDay = cal.getTimeInMillis() / 1000;
        return lastDay;
    }

    /**
     *
     * @author yumo.lck
     * @param chooseFirstDay
     */

    public static long getBeforeMonthDay(long day, boolean chooseFirstDay) {
        long chooseDay = 0L;
        int currentMonth = getMonth(day);
        int currentYear = getYear(day);
        if (currentMonth > 1) {
            currentMonth--;
        } else {
            currentYear--;
            currentMonth = 12;
        }
        if (chooseFirstDay) {
            chooseDay = getFirstDay4Month(currentYear, currentMonth);
            return chooseDay;
        } else {
            chooseDay = getLastDay4Month(currentYear, currentMonth);
            return chooseDay;
        }

    }

    /**
     * @return long
     */
    public static long getMillByOneDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     *
     * @return long
     */
    public static long getMillByYesDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }

    /**
     *
     * @return
     */
    public static long getMillByLastWeekDay() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - 7);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis() / 1000;
    }
    
   /**
    * @return long
    */
    public static long getMillByDay(int severalDays,String condition) {
    	int dateT=0;
        Calendar cal = Calendar.getInstance();
    	if(condition==null){
    		return getMillToDay(cal,dateT);
    	}
        if(condition.equals("-")){
        	dateT = (cal.get(Calendar.DATE) - severalDays);
        	return getMillToDay(cal,dateT);
        }
        if(condition.equals("+")){
        	dateT = (cal.get(Calendar.DATE) + severalDays);
        	return getMillToDay(cal,dateT);
        }
		return getMillToDay(cal,dateT);
  }
    
    /**
     * @return long
     */
    public static long getStampByDay(int severalDays,String condition) {
    	int dateT=0;
    	Calendar cal = Calendar.getInstance();
    	if(condition==null){
    		return getStampToDay(cal,dateT);
    	}
    	if(condition.equals("-")){
    		dateT = (cal.get(Calendar.DATE) - severalDays);
    		return getStampToDay(cal,dateT);
    	}
    	if(condition.equals("+")){
    		dateT = (cal.get(Calendar.DATE) + severalDays);
    		return getStampToDay(cal,dateT);
    	}
    	return getStampToDay(cal,dateT);
    }
    /**
     * @return long
     */
    public static long getMillByDay(){
		return getMillByDay(0,null);
    }
    
    /**
     * @param cal  Calendar
     * @param dateT Integer 
     * @return  long
     */
    public static long getMillToDay(Calendar cal,int dateT){
		   if(dateT!=0){
			   cal.set(Calendar.DATE, dateT);
		   }
	       cal.set(Calendar.HOUR_OF_DAY, 0);
	       cal.set(Calendar.MINUTE, 0);
	       cal.set(Calendar.SECOND, 0);
	       cal.set(Calendar.MILLISECOND, 0);
	       return cal.getTimeInMillis()/1000;
	}
    
    /**
     * @param cal  Calendar
     * @param dateT Integer 
     * @return  long
     */
    public static long getStampToDay(Calendar cal,int dateT){
    	if(dateT!=0){
    		cal.set(Calendar.DATE, dateT);
    	}
    	return cal.getTimeInMillis();
    }

    public static String getToday() {
        Calendar cal = Calendar.getInstance();
        return cal.get(1) + "年" + cal.get(2) + "月" + cal.get(3) + "日";
    }

    /**
     * @param day
     * @return format time
     */
    public static String getDate(long day, String format) {
        Calendar cal = Calendar.getInstance();
        if (("" + day).length() > 10) {
            cal.setTime(new Date(day));
        } else {
            cal.setTime(new Date(day * 1000));
        }
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(cal.getTime());
    }
    
    /**
     *
     * @param  date
     * @return
     */
    public static String getDate(Date date, String format) {
        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(date);
    }
    
    
    /**
     *
     * @param day
     * @param format
     * @return long
     * @throws ParseException 
     */
    public static long stringToLong(String day, String format) throws ParseException {
    	SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        long Date = dateFormat.parse(day).getTime();
    	return Date;
    }
    
    /**
     * @param day
     * @param format
     * @return Date
     * @throws ParseException
     */
    public static Date stringToDate(String day, String format)  {
    	try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(format);
			 Date Date = dateFormat.parse(day);
			return Date;
		} catch (ParseException e) {
			return new Date();
		}
    }
    
    
    /**
     * long型时间戳转为String型
     * 
     * @param day 秒
     * @return 格式化后的日期
     * @throws ParseException 
     */
    public static String longToString(long day, String format) throws ParseException {
    	if (("" + day).length() <= 10){
            day=day*1000;
        }
    	SimpleDateFormat dateFormat = new SimpleDateFormat(format);
	    String Date = dateFormat.format(day);
    	return Date;
    }

    /**
     *
     * @param day 秒
     * @param minusDay 需要减掉的天数
     * @return 秒
     */
    public static int getMinusDate(int day, int minusDay) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(day * 1000));
        cal.set(Calendar.DATE, cal.get(Calendar.DATE) - minusDay);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return (int) cal.getTimeInMillis() / 1000;
    }

    /**
     *
     * @return long
     */
    public static long getMillByNow() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        return cal.getTimeInMillis();
    }

	public static int getWeeksBetweenTwoDates(long startDay, long endDay) {
		int week = getWeekOfYear(endDay) - getWeekOfYear(startDay) + 1;
		if(week<1){
			week = getWeekOfYear(endDay) + getMaxWeekOfYear(startDay) - getWeekOfYear(startDay) + 1;
		}
		return week;
	}

	public static int getMaxWeekOfYear(long startDay) {
		Calendar cal = Calendar.getInstance();
        cal.setTime(new Date(startDay * 1000));
        return cal.getMaximum(Calendar.WEEK_OF_YEAR);
	}
	
	public static int getMonthsBetweenTwoDates(long startDay, long endDay) {
		int month = DateUtil.getMonth(endDay) - DateUtil.getMonth(startDay) + 1;
		if(month<1){
			month = getMonth(endDay) + 12 - getMonth(startDay) +1;
		}
		return month;
	}
	
	public static Date parseDate(String dateStr, String pattern){
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern(pattern);
		try {
			return sdf.parse(dateStr);
		} catch (ParseException e) {
			return null;
		}
	}
	
	/**
     *
     * @param time Long 时间
     * @return long
     */
    public static long getMinuteStart(long time) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + time).length() > 10) {
            cal.setTime(new Date(time));
        } else {
            cal.setTime(new Date(time * 1000));
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }
    
    /**
     * @param time Long
     * @return long
     */
    public static long getHourStart(long time) {
        long firstDay = 0L;
        Calendar cal = Calendar.getInstance();
        if (("" + time).length() > 10) {
            cal.setTime(new Date(time));
        } else {
            cal.setTime(new Date(time * 1000));
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        firstDay = cal.getTimeInMillis() / 1000;
        return firstDay;
    }

    /**
     * @param time
     * @return Date
     */
    public static Date getDateByLong(long time){
        Date date = new Date();
        date.setTime(time);
        return date;
    }
    

    public static Date parseDate(String dateStr, String pattern, Locale locale){
    	SimpleDateFormat df = new SimpleDateFormat(
				pattern, locale);

		df.setTimeZone(new SimpleTimeZone(0, "GMT"));
		try {
			return df.parse(dateStr);
		} catch (ParseException e) {
			return null;
		}
	}
    
    public static String getDate(Date date, String format, Locale locale) {
    	SimpleDateFormat df = new SimpleDateFormat(
    			format, locale);
    	df.setTimeZone(new SimpleTimeZone(0, "GMT"));
        return df.format(date);
    }

    public static Timestamp columnToTimestamp(Object column) {
        if (column == null) {
            return null;
        } else if(column instanceof String) {
            return new Timestamp(stringToDate((String)column).getTime());
        } else if (column instanceof Integer) {
            Integer rawData = (Integer) column;
            return new Timestamp(rawData.longValue());
        } else if (column instanceof Long) {
            Long rawData = (Long) column;
            return new Timestamp(rawData.longValue());
        } else if (column instanceof java.sql.Date) {
            return (Timestamp) column;
        } else if(column instanceof Timestamp) {
            return (Timestamp) column;
        } else if(column instanceof Date) {
            Date d = (Date)column;
            return new Timestamp(d.getTime());
        }

        throw new IllegalArgumentException("Can't convert " + column.getClass().getName() + " to Date");
    }

    public static String dateToString(Date date) {
        return dateFormatter.format(date);
    }

    public static String timestampToString(Date date) {
        return datetimeFormatter.format(date);
    }


    public static Timestamp getTimestampFromStr(String timeStr) {
        if (DATETIME.matcher(timeStr).matches()) {
            Instant instant = Instant.from(ISO_INSTANT.parse(timeStr));
            return new Timestamp(instant.getEpochSecond() * MILLIS_PER_SECOND);
        } else {
            java.sql.Date date = null;
            try {
                date = new java.sql.Date(datetimeFormatter.parse(timeStr).getTime());
            } catch (ParseException e) {
                throw new RuntimeException("getTimestampFromStr error data is " + timeStr);
            }
            return new Timestamp(date.getTime());
        }
    }

    public static java.sql.Date getDateFromStr(String dateStr) {
        // 2020-01-01 format
        if (DATE.matcher(dateStr).matches()) {
            // convert from local date to instant
            Instant instant = LocalDate.parse(dateStr).atTime(LocalTime.of(0, 0, 0, 0)).toInstant(ZoneOffset.UTC);
            // calculate the timezone offset in millis
            int offset = TimeZone.getDefault().getOffset(instant.toEpochMilli());
            // need to remove the offset since time has no TZ component
            return new java.sql.Date(instant.toEpochMilli() - offset);
        } else if (DATETIME.matcher(dateStr).matches()) {
            // 2020-01-01T12:12:12Z format
            Instant instant = Instant.from(ISO_INSTANT.parse(dateStr));
            return new java.sql.Date(instant.toEpochMilli());
        } else {
            try {
                // 2020-01-01 12:12:12.0 format
                return new java.sql.Date(datetimeFormatter.parse(dateStr).getTime());
            } catch (ParseException e) {
                throw new RuntimeException("String convert to Date fail.");
            }
        }
    }


    public static String getStringFromTimestamp(Timestamp timestamp) {
        return datetimeFormatter.format(timestamp);
    }

    public static String getStringFromDate(java.sql.Date date) {
        return dateFormatter.format(date);
    }

}