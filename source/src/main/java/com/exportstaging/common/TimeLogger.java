package com.exportstaging.common;

public class TimeLogger {
    public void timeFormatter(long totalTime, String sMessage) {
        int iTotalMilSec = (int) totalTime;
        int iMilSec;
        int iSec = iTotalMilSec / 1000;
        int iMin = iSec / 60;
        int iHour = iMin / 60;
        iMin = iMin % 60;
        iSec = iSec % 60;
        iMilSec = iTotalMilSec % 1000;
        System.out.println(sMessage + ": " + iHour + "h " + iMin + "m " + iSec + "s " + iMilSec + "ms");
    }
}

