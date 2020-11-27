package com.exportstaging.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The responsibilities of this class is to find the average time taken by the elastic and master subscriber
 * Output will be shown on console as its not official file
 * This class file can be used to find the average after initial export is done or while performance testing
 */
public class TookTimeCalculator {

    private static final String ES_MASTER_SUBSCRIBER_IDENTIFIER  = "[MasterSubscriber] Successfully updated data with id";
    private static final String ES_ELASTIC_SUBSCRIBER_IDENTIFIER = "[ElasticSubscriber] Successfully updated data with id";
    private static final String ES_MASTER_SUBSCRIBER_NAME  = "Cassandra";
    private static final String ES_ELASTIC_SUBSCRIBER_NAME = "Elasticsearch";
    private static final String ES_DIRECTORY_LOG_PATH = "C:\\Users\\kiran.gosavi\\Desktop\\Export Performance\\ExportDatabase\\Export\\INFO\\2018\\2018-10";

    public static void main(String[] args) {
        File[] fileList = getFileList(getDirectoryPath());
        computeTime(getExportResult(fileList));
    }


    /**
     * As per the Export subscriber result mean time will be calculated
     *
     * @param exportResult Map containing the information about the subscriber
     */
    private static void computeTime(Map<String, Map<Integer, Integer>> exportResult) {
        for (Map.Entry<String, Map<Integer, Integer>> letterEntry : exportResult.entrySet()) {
            int cassandraMeanTime = getMeanTime(letterEntry.getValue());
            printDetails(letterEntry.getKey(), cassandraMeanTime);
        }
    }


    /**
     * Method will return the directory path of log
     *
     * @return string directory path
     */
    private static String getDirectoryPath() {
        return ES_DIRECTORY_LOG_PATH;
    }


    /**
     * As per the computation logs will be shown on console
     *
     * @param serviceName string name of service/subscriber
     * @param meanTime    int time that we have calculated
     */
    private static void printDetails(String serviceName, int meanTime) {
        System.out.println("Total Mean Time For " + serviceName + " Is : " + meanTime + "ms");
    }


    /**
     * As per the subscriber details mean time will be calculated
     *
     * @param subscriberDetails Map containing the information about the subscriber
     * @return int mean time
     */
    private static int getMeanTime(Map<Integer, Integer> subscriberDetails) {
        Map.Entry<Integer, Integer> result = subscriberDetails.entrySet().iterator().next();
        int time = result.getKey() / result.getValue();

        return time;
    }


    /**
     * As per the provided directory path all the first level file will be fetched
     *
     * @param directoryPath string directory path
     * @return array of file list
     */
    private static File[] getFileList(String directoryPath) {
        File directory = new File(directoryPath);
        File[] listFiles = new File[0];
        if (directory.isDirectory()) {
            listFiles = directory.listFiles();
        }

        return listFiles;
    }


    /**
     * As per the provided array of log files subscriber will find the total successful item count
     * and addition of took time for insertion
     *
     * @param filePath array of file list
     * @return Map containing information about the subscriber with total time and no. of successful insertion
     */
    private static Map<String, Map<Integer, Integer>> getExportResult(File[] filePath) {
        int cassandraTimeInMiliSec = 0;
        int elasticTimeInMiliSec = 0;
        int elasticLineCount = 0;
        int cassandraLineCount = 0;
        Map<String, Map<Integer, Integer>> ExportResult = new HashMap();
        for (File path : filePath) {
            try (BufferedReader br = new BufferedReader(new FileReader(path.getPath()))) {
                String currentLine;
                while ((currentLine = br.readLine()) != null) {
                    if (currentLine.contains(ES_MASTER_SUBSCRIBER_IDENTIFIER)) {
                        int timeInMilSec = getTimeInMilliSec(currentLine);
                        cassandraTimeInMiliSec = cassandraTimeInMiliSec + timeInMilSec;
                        cassandraLineCount++;
                    } else if (currentLine.contains(ES_ELASTIC_SUBSCRIBER_IDENTIFIER)) {
                        int timeInMilSec = getTimeInMilliSec(currentLine);
                        elasticTimeInMiliSec = elasticTimeInMiliSec + timeInMilSec;
                        elasticLineCount++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Map<Integer, Integer> tempCassandra = new HashMap<>();
        tempCassandra.put(cassandraTimeInMiliSec, cassandraLineCount);
        Map<Integer, Integer> tempElastic = new HashMap<>();
        tempElastic.put(elasticTimeInMiliSec, elasticLineCount);
        ExportResult.put(ES_MASTER_SUBSCRIBER_NAME, tempCassandra);
        ExportResult.put(ES_ELASTIC_SUBSCRIBER_NAME, tempElastic);

        return ExportResult;
    }


    /**
     * get time in millisecond from each line
     *
     * @param currentLine String line having the information about time
     *
     * @return total took time in millisecond
     */
    private static int getTimeInMilliSec(String currentLine) {
        String[] words = currentLine.split("\\s");
        int timeInMilSec = Integer.parseInt(words[15]);

        return timeInMilSec;
    }
}
