package com.exportstaging.common;

import org.springframework.stereotype.Component;

@Component("unitConverter")
public class UnitConverter {
    public static final int BYTE = 0;
    public static final int KILOBYTE = 1;
    public static final int MEGABYTE = 2;
    public static final int GIGABYTE = 3;
    public static final int TERABYTE = 4;
    public static final int PETABYTE = 5;
    public static final int EXABYTE = 6;
    public static final int ZETTABYTE = 7;
    public static final int YOTTABYTE = 8;
    private final String[] unitList = new String[]{"Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};

    public String convertMemory(long value, int unit) {
        long convertedBytes = value / 1024;
        if (convertedBytes > 0) {
            return convertMemory(convertedBytes, unit + 1);
        } else {
            return value + " " + unitList[unit];
        }
    }
}
