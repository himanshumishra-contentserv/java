package com.exportstaging.common;

import com.exportstaging.api.exception.ExportStagingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.Inflater;

public class ExportMiscellaneousUtils {

    public final static int CONSTANT_EXPORT_TYPE_INITIAL = 0;
    public final static int CONSTANT_EXPORT_TYPE_DELTA   = 1;
    public final static int MYSQL_ACTION_DELETE          = 2;

    //Header JSON Fields
    public static final String ES_FIELDS_SEARCHABLE            = "SearchableFields";
    public static final String ES_FIELDS_STANDARD              = "StandardFields";
    public static final String ES_FIELDS_CUSTOM                = "CustomFields";
    public static final String ES_FIELDS_SEARCHABLE_REFERENCES = "SearchableReferences";
    public static final String ES_FIELDS_SEARCHABLE_SUBTABLES  = "SearchableSubtables";
    public static final String CS_TYPE_HTML = "html";

    //Property file name constants
    public static final String ACTIVEMQ_PROPERTIES      = "activemq.properties";
    public static final String CASSANDRA_PROPERTIES     = "cassandra.properties";
    public static final String CORE_PROPERTIES          = "core.properties";
    public static final String ELASTICSEARCH_PROPERTIES = "elasticsearch.properties";
    public static final String MYSQL_PROPERTIES         = "mysql.properties";

    //Generic constants
    public static final String EXPORT_DATABASE_DATA_TYPE_TEXT       = "text";
    public static final String EXPORT_DATABASE_DATA_TYPE_INT        = "int";
    public static final String EXPORT_DATABASE_DATA_TYPE_BIGINT     = "bigint";
    public static final String EXPORT_DATABASE_DATA_TYPE_FLOAT      = "float";
    public static final String EXPORT_DATABASE_DATA_TYPE_DOUBLE     = "double";
    public static final String EXPORT_DATABASE_DATA_TYPE_TIMESTAMP  = "timestamp";
    public static final String EXPORT_DATABASE_DATA_TYPE_LIST_INT   = "list<int>";
    public static final String EXPORT_ITEM_TYPE_USER                = "User";
    public static final String EXPORT_ITEM_TYPE_WORKFLOW            = "Workflow";
    public static final String EXPORT_ITEM_TYPE_LANGUAGE            = "Language";
    public static final String EXPORT_ITEM_TYPE_MAMFILE             = "Mamfile";
    public static final String EXPORT_ITEM_TYPE_PDMARTICLE          = "Pdmarticle";
    public static final String EXPORT_ITEM_TYPE_PDMARTICLESTRUCTURE = "Pdmarticlestructure";
    public static final String EXPORT_ITEM_TYPE_MAMFILECONTENT      = "MamfileContent";
    
    public static final String EXPORT_CONFIGURED_DYNAMIC_ATTRIBUTES = "export.configured.dynamic.attributes.";


    /*
      Constant for elastic search exception message
     */
    public static final String EXPORT_ELASTIC_FIELD_LIMIT             = "Increased index level limit of total fields for " +
                                                                        "inserting data by :";
    public static final String EXPORT_ELASTIC_FIELD_LIMIT_MESSAGE     = "Limit of total fields";
    public static final String EXPORT_ELASTIC_INDEX_NOT_FOUND_MESSAGE = "no such index";

    /*
      Constant for elastic search properties
     */
    public static final String EXPORT_ELASTIC_CLUSTER_NAME                   = "cluster.name";
    public static final String EXPORT_ELASTIC_SHARD_NUMBER                   = "number_of_shards";
    public static final String EXPORT_ELASTIC_REPLICA_NUMBER                 = "number_of_replicas";
    public static final String EXPORT_ELASTIC_SETTING_FIELD_LIMIT            = "index.mapping.total_fields.limit";
    public static final String EXPORT_ELASTIC_SETTING_MAX_RESULT_WINDOW      = "index.max_result_window";
    public static final String EXPORT_ELASTIC_SETTING_INDEX_REFRESH_INTERVAL = "index.refresh_interval";


    /*
     Constants for startup param
    */
    public static final String EXPORT_ELASTIC_INDEX_UPDATER = "elasticIndexUpdater";
    public static final String EXPORT_MODE                  = "-mode";
    public static final String EXPORT_PROJECT_NAME          = "-p";
    public static final String EXPORT_LOG_PATH              = "-logPath";
    public static final String EXPORT_PROPERTIES_FILE_PATH  = "-propertiesPath";
    public static final String EXPORT_LOGGER_NAME           = "exportstaging";

    /**
     * Constants for PORT number information of Cassandra & Elastic Search
     */
    public static final int DEFAULT_TCP_PORT_CASSANDRA     = 9042;
    public static final int DEFAULT_TCP_PORT_ELASTICSEARCH = 9300;
    public static final int DEFAULT_PORT_ACTIVEMQ          = 61616;
    public static final int DEFAULT_PORT_MYSQL             = 3306;

    public static final String TAB_DELIMITER                      = "\t";
    public static final String EXPORT_DATABASE_LOG_ITEM_TYPE      = "itemType";
    public static final String EXPORT_DATABASE_LOG_OPERATION_TYPE = "operationType";
    public static final String EXPORT_DATABASE_LOG_TRACE_ID       = "traceID";

    public static final String CONSTANT_ITEM_TYPE                       = "ItemType";
    public static final String CONSTANT_TYPE                            = "Type";
    public static final String CONSTANT_EXPORT_TYPE                     = "ExportType";
    public static final String IDB_INSERT_TIME                          = "IDBInsertTime";
    public static final String EXPORT_FIELD_STATEID                     = "StateID";
    public static final String EXPORT_FIELD_WORKFLOWID                  = "WorkflowID";
    public static final String EXPORT_FIELD_LANGUAGEID                  = "LanguageID";
    public static final String EXPORT_FIELD_ID                          = "ID";
    public static final String EXPORT_TYPE_OPERATION                    = "Operation";
    public static final String EXPORT_IDB_TABLE_SUBSCRIBER_ITEM_MAPPING = "subscriber_item_mapping";
    public static final String EXPORT_DATABASE_FIELD_CREATION_DATE      = "_InsertTime";


    /**
     * Constant related to storage specifications
     **/
    public static final String EXPORT_DB_TYPE_CASSANDRA  = "Cassandra";
    public static final String EXPORT_DB_TYPE_SCYLLA     = "ScyllaDB";
    public static final String EXPORT_DB_SYSTEM          = "system";
    public static final String EXPORT_TABLE_SCYLLA_LOCAL = "scylla_local";


    /**
     * Constant for read/write operation on cassandra to set timeout
     */
    public static final int CONST_READ_TIMEOUT_IN_MILISEC = 1800000;

    //Private constants
    private static final String EXPORT_DATABASE_FIELD_SUBITEMID     = "_SubitemID";
    private static final String EXPORT_DATABASE_FIELD_TYPE_ID       = "_TypeID";
    private static final String EXPORT_DATABASE_FIELD_SUBATTRIBUTES = "_SubAttributes";
    private static final String EXPORT_DATABASE_FIELD_MODULE        = "_Module";

    public static final String EXPORT_JSON_KEY_ITEM       = "Item";
    public static final String EXPORT_JSON_KEY_RECORD     = "Record";
    public static final String EXPORT_JSON_KEY_DEBUG_MODE = "DebugMode";
    public static final String EXPORT_JSON_KEY_REINDEX    = "Reindex";

    private static volatile List<String> configuredTypes;
    private static volatile List<String> itemTypes;
    private static volatile List<String> recordTypes;

    /**
     * Private Static constants that cache values for different paths
     */
    private static String projectName;
    private static String propertiesPath;
    private static String dataPath;
    private static String logPath;
    private static String pidFilePath;
    public static final String PROPERTIES = "properties";

    private static File rootFile;
    public static final String EXPORT_ELASTIC_XPACK_SECURITY_USER = "xpack.security.user";

    private static String[] configPath        = new String[]{"data", "config.php"};
    private static String[] basePath          = new String[]{"data", "exportstaging", "config"};
    private static String[] internalLogPath   = new String[]{"data", "logs", "processes", "ExportDatabase"};
    private static String[] externalLogPath   = new String[]{"logs"};
    private static String[] internalDataPath  = new String[]{"ExportData", "data"};
    private static String[] externalDataPath  = new String[]{"data"};
    private static String[] exportPidFilePath = new String[]{"ExportExecutor.pid"};

    private final static Logger logger = Logger.getLogger("exportstaging");
    private static Map<String, String> projectDetails = new HashMap<>();

    public static void isProjectNameLoaded(String projectName) {
        if (!projectName.equals(getPropertyValue("core.project.name", "core.properties", projectName))) {
            System.out.println(projectName + " is invalid project name, please provide valid name and rerun again.");
            System.exit(0);
        }
        ExportMiscellaneousUtils.projectName = projectName;
    }

    /**
     * @param projectName Current runnig project name.
     * @return rootFile if ExportExecutor is not started from lib otherwise null
     */
    public static File getRootFile(String projectName) {
        if (rootFile == null) {
            File root = new File(Paths.get(".").toAbsolutePath().normalize().toString());
            String rootAbsolutePath = root.getAbsolutePath();
            if (projectName != null && rootAbsolutePath.contains(projectName)) {
                root = new File(rootAbsolutePath.substring(0, rootAbsolutePath.lastIndexOf(projectName)));
            } else if (root.getName().equals("source") && rootAbsolutePath.contains("admin")) {
                root = new File(rootAbsolutePath.substring(0, rootAbsolutePath.lastIndexOf("admin")));
            } else if (rootAbsolutePath.contains("lib")) {
                String path = rootAbsolutePath.substring(0, rootAbsolutePath.lastIndexOf("lib"));
                root = new File(path.substring(0, path.lastIndexOf(File.separator)));
                if (root.getName().equals("admin.local")) {
                    root = root.getParentFile();
                }
            } else if (rootAbsolutePath.contains("admin") && rootAbsolutePath.contains("dist")) {
                root = new File(rootAbsolutePath.substring(0, rootAbsolutePath.lastIndexOf("admin")));
            } else {
                root = getPath(root.listFiles());
            }
            rootFile = root;
        }
        return rootFile;
    }

    private static File getPath(File[] files) {
        for (File file : files) {
            String name = file.getName();
            if (file.isDirectory() && !name.startsWith(".")) {
                File[] a = file.listFiles();
                if (!name.equals("admin")) {
                    return getPath(a);
                } else {
                    return file.getParentFile();
                }
            }
        }
        return null;
    }

    public static String getPropertyValue(String key, String propertyType) {

        URL filePath = ClassLoader.getSystemResource(File.separator + "properties" + File.separator + propertyType);
        return readFromFile(filePath.getPath(), key);
    }

    public static String getPropertyValue(String key, String propertyType, String projectName) {
        return readFromFile(getProjectPropertiesPath(projectName) + File.separator + propertyType, key);
    }

    private static String readFromFile(String filePath, String key) {
        String value;
        return ((value = getProperties(filePath).getProperty(key)) == null || "".equals(value)) ? "" : value;
    }


    private static Properties getProperties(String filePath) {
        Properties properties = new Properties();
        try {
            FileInputStream stream = new FileInputStream(filePath);
            properties.load(stream);
            stream.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return properties;
    }

    public static String getExportDatabaseFieldSubitemid() {
        return EXPORT_DATABASE_FIELD_SUBITEMID;
    }

    public static String getExportDatabaseFieldTypeId() {
        return EXPORT_DATABASE_FIELD_TYPE_ID;
    }

    public static String getExportDatabaseFieldModule() {
        return EXPORT_DATABASE_FIELD_MODULE;
    }

    public static String getExportDatabaseFieldSubAttributes() {
        return EXPORT_DATABASE_FIELD_SUBATTRIBUTES;
    }

    public static List<String> getCoreItemTypes() {
        if (itemTypes == null) {
            refreshItemTypeCache();
        }
        return itemTypes;
    }

    public static List<String> getCoreRecordTypes() {
        if (recordTypes == null) {
            refreshItemTypeCache();
        }
        return recordTypes;
    }

    public static List<String> getConfiguredTypes() {
        if (configuredTypes == null) {
            refreshItemTypeCache();
        }
        return configuredTypes;
    }

    /**
     * Converts comma separated string into @List
     * @param configurationTypes String of Item types
     * @return @List of item types
     */
    public static List<String> splitItemTypes(String configurationTypes) {
        List<String> data = new ArrayList<>();
        if (configurationTypes.length() > 0) {
            data.addAll(Arrays.asList(configurationTypes.replaceAll(" ", "").split(",")));
            if (data.contains(EXPORT_ITEM_TYPE_LANGUAGE)) {
                data.remove(EXPORT_ITEM_TYPE_LANGUAGE);
                data.add(0, EXPORT_ITEM_TYPE_LANGUAGE);
            }
        }
        return data;
    }

    public static void refreshItemTypeCache() {
        configuredTypes = splitItemTypes(getPropertyValue("export.configured.types", CORE_PROPERTIES, projectName));
        itemTypes = splitItemTypes(getPropertyValue("export.core.itemtypes", CORE_PROPERTIES, projectName));
        recordTypes = splitItemTypes(getPropertyValue("export.core.recordtypes", CORE_PROPERTIES, projectName));
    }

    /**
     * Convert list of elements to lowercase.
     *
     * @param list List of elements
     * @return Converted list
     */
    public static List<String> convertListElementToLowerCase(List<String> list) {
        List<String> convertedList = new ArrayList<>();
        for (String value : list) {
            convertedList.add(value.toLowerCase());
        }
        return convertedList;
    }

    /**
     * Converting first letter in capital
     *
     * @param list List of item types
     * @return List of item types with first letter in capital
     */
    public static List<String> convertFirstLetterToUpperCase(List<String> list) {
        return list.stream().filter(value -> value.length() > 0).map(value -> value.substring(0, 1).toUpperCase() + value.substring(1)).collect(Collectors.toList());
    }

    public static String readFile(String path, Charset encoding) throws IOException {
        // as per the community, this the fastest way to read file
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }


    /**
     * As per provided connection URL, host and port will be provided
     * Method also expecting default port number so that if port is not mentioned then default port will be set
     *
     * @param connectionUrl string connection URL contains host and port information
     * @param defaultPort   int default port number of Cassandra/Elastic
     * @return ipPortMapping Map of Ip address and Port number
     * @throws UnknownHostException will throws an exception if host not found
     */
    public static Map<InetAddress, Integer> getURLDetails(String connectionUrl, int defaultPort) throws UnknownHostException {
        List<String> connectionList = Arrays.asList(connectionUrl.split(","));
        Map<InetAddress, Integer> ipPortMapping = new HashMap();
        for (String url : connectionList) {
            String ipPortPair[] = url.split(":");
            String ipAddress = ipPortPair[0].trim();
            int port = ((ipPortPair.length == 1) ? defaultPort : Integer.parseInt(ipPortPair[1].trim()));

            InetAddress inetAddress = InetAddress.getByName(ipAddress);
            ipPortMapping.put(inetAddress, port);
        }

        return ipPortMapping;
    }


    public static boolean isExecutorRunningExternally(String projectName) {
        File root = getRootFile(projectName);
        if (root != null) {

            File[] files;
            try {
                files = root.listFiles();
                for (File file : files) {
                    if (file.getName().equals(projectName)) {
                        if (new File(file.getPath() + getConfigFilePostfixPath()).exists()) {
                            return false;
                        }
                    }
                }
            } catch (NullPointerException e) {
                System.out.println("NullPointerException while checking if Jar is running in ContentServ environment.");
            } catch (Exception e) {
                System.out.println("Error while checking if Jar is in ContentServ environment. Error Cause: " + e.getMessage());
            }
        }
        return true;
    }

    public static String getLogFolderPath(String projectName) {
        if (logPath == null) {
            File root = getRootFile(projectName);
            if (root != null) {
                logPath = root.getPath();
                if (isExecutorRunningExternally(projectName)) {
                    logPath += File.separator + "logs" + File.separator + projectName + stickPathPostFix(externalLogPath) + File.separator;
                } else {
                    logPath += File.separator + projectName + stickPathPostFix(internalLogPath) + File.separator;
                }
            }
        }
        return logPath;
    }

    public static String getProjectPropertiesPath(String projectName) {
        if (propertiesPath == null) {
            propertiesPath = getProjectPropertiesDirectoryPath(projectName);
        }
        return propertiesPath;
    }

    public static String getConfigFilePostfixPath() {

        return stickPathPostFix(configPath);
    }

    public static String getDataFolderPath(String projectName) {
        if (dataPath == null) {
            File root = getRootFile(projectName);
            if (root != null) {
                dataPath = root.getPath();
                if (isExecutorRunningExternally(projectName)) {
                    dataPath += File.separator + "data" + File.separator + projectName + stickPathPostFix(externalDataPath);
                } else {
                    dataPath += File.separator + projectName + stickPathPostFix(basePath) + stickPathPostFix(internalDataPath);
                }
            }
        }
        return dataPath;
    }

    public static String getExportExecutorProcessIDFilePath(String projectName) {
        if (pidFilePath == null) {
            File root = getRootFile(projectName);
            if (root != null) {
                pidFilePath = root.getPath() + File.separator + projectName + stickPathPostFix(basePath) + stickPathPostFix(exportPidFilePath);
            }
        }
        return pidFilePath;
    }

    private static String stickPathPostFix(String[] path) {
        return File.separator + String.join(File.separator, path);
    }

    public static ArrayList<String> getProjectNames() throws ExportStagingException {
        return new ArrayList<String>(getProjectDetails().values());
    }

    /**
     * Method will provide the details of project
     * Map will contains export database name as a key and project name as a value
     * In case of multi-project will get multiple entries in the map
     *
     * @return Map of export database name and project name
     * @throws ExportStagingException will throws an exception if anything goes wrong while preparing the project details
     */
    public static Map<String, String> getProjectDetails() throws ExportStagingException {
        if(projectDetails.isEmpty()) {
            File root = getRootFile(projectName);
            if (root == null) {
                throw new ExportStagingException("Project names cannot be determined");
            }
            File[] files = root.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    File configFile = new File(file.getAbsolutePath() + File.separator + "data" + File.separator + "config.php");
                    if (configFile.canRead()) {
                        String localProjectName = file.getName();
                        String propertiesPath = getProjectPropertiesDirectoryPath(
                          localProjectName) + File.separator + CORE_PROPERTIES;
                        String exportDatabaseName = readFromFile(propertiesPath, "export.database.name");
                        if (StringUtils.isNotEmpty(exportDatabaseName)) {
                            projectDetails.put(exportDatabaseName, localProjectName);
                        }
                    }
                }
            }
        }

        return projectDetails;
    }

    public static void setProjectName(String projectName) {
        ExportMiscellaneousUtils.projectName = projectName;
    }

    public static void setPropertiesPath(String propertiesPath) {
        ExportMiscellaneousUtils.propertiesPath = propertiesPath;
    }

    public static void setLogPath(String logPath) {
        ExportMiscellaneousUtils.logPath = logPath;
    }

    /**
     * Check the provided date is valid or not based on given date format
     *
     * @param dateToValidate Date for validation
     * @param dateFormat     Format for validating the date
     * @return true if provided date is valid for given format otherwise false
     */
    public boolean isDateValid(String dateToValidate, String dateFormat) {
        if (dateToValidate == null) {
            return false;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        sdf.setLenient(false);
        try {
            sdf.parse(dateToValidate);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    /**
     * Changes the key to Value and Value to Key
     *
     * @param map The map that needs to be reversed
     * @param <V> Data type of the keys
     * @param <K> Data type of the values
     * @return Reversed map with values as keys and keys as values
     */
    public static <V, K> HashMap<V, K> reverseMap(Map<K, V> map) {
        HashMap<V, K> rev = new HashMap<>();
        for (Map.Entry<K, V> entry : map.entrySet())
            rev.put(entry.getValue(), entry.getKey());
        return rev;
    }

    /**
     * Returns which operation perform while exporting data for logger.
     *
     * @param action      Action should be 1 for update 2 for Delete and 3 for create.
     * @param jmsPriority Activemq jms priority to process message if priority is 7,8,9 then operation will be initial otherwise delta.
     * @param type        Based on CS record type like Item,Configuration or mapping.
     * @return Operation type based on given parameter.
     */
    public static String getOperationType(int action, int jmsPriority, String type) {
        String operationType = "";
        if (jmsPriority == 7 || jmsPriority == 8 || jmsPriority == 9) {
            operationType = "Initial";
        } else {
            switch (action) {
                case 1:
                    operationType = "Update";
                    break;
                case 2:
                    operationType = "Delete";
                    break;
                case 3:
                    operationType = "Create";
                    break;
            }
        }
        return operationType + type;
    }

    /**
     * Returns true if the operating system is Windows
     *
     * @return true if current operating system is Windows else returns false
     */
    public static boolean isWindows() {
        String osName = System.getProperty("os.name").toLowerCase();
        return (osName.contains("win"));
    }


    /**
     * Convert string to List type
     *
     * @param configurationTypes Types of the configurations
     * @return data List of the Configurations
     */
    public static List<String> convertStringToList(String configurationTypes) {
        List<String> data = new ArrayList<>();
        if (configurationTypes.length() > 0) {
            data.addAll(Arrays.asList(configurationTypes.replaceAll(" ", "").split(",")));
            if (data.contains(EXPORT_ITEM_TYPE_LANGUAGE)) {
                data.remove(data.indexOf(EXPORT_ITEM_TYPE_LANGUAGE));
                data.add(0, EXPORT_ITEM_TYPE_LANGUAGE);
            }
        }
        return data;
    }


    /**
     * Returns export database project side properties path
     *
     * @param projectName For which project need properties path
     * @return Export database Properties directory path
     */
    private static String getProjectPropertiesDirectoryPath(String projectName) {
        String path = null;
        File root = getRootFile(projectName);
        if (root != null) {
            path = root.getPath();
            if (isExecutorRunningExternally(projectName)) {
                path += File.separator + "data" + File.separator + projectName + File.separator + PROPERTIES;
            } else {
                path += File.separator + projectName + stickPathPostFix(basePath) + File.separator + PROPERTIES;
            }
        }
        return path;
    }
    public static String getContentServPath() {
        String exportExecutorJarPath = getJarFilePath();
        if (!exportExecutorJarPath.isEmpty()) {
            int adminIndex = exportExecutorJarPath.lastIndexOf("admin");
            if (adminIndex != -1) {
                return exportExecutorJarPath.substring(0, adminIndex);
            } else {
                return exportExecutorJarPath;
            }
        }
        return "";
    }

    private static String getJarFilePath() {
        try {
            String path = URLDecoder.decode(ExportMiscellaneousUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath(), "UTF-8");
            File file = new File(path);
            if (path.contains(".jar")) { //Handling for Jar Execution
                return file.getParent();
            }
            return file.getPath();
        } catch (UnsupportedEncodingException e) {
            logger.error("[ExportDatabase] UnsupportedEncodingException while loading the jar path" + e.getMessage());
        } catch (Exception e) {
            logger.error("[ExportDatabase] Exception while loading the jar path" + e.getMessage());
        }
        return "";
    }

  /**
   * provided encoded compressed string will be decompressed
   *
   * @param compressedMessage String contain compressed message
   *
   * @return encoded compressed string will be decompressed
   */
  public static String getUnCompressedItemMessage(String compressedMessage)
  {
    String                unCompressedMessage = null;
    ByteArrayOutputStream outputStream        = null;
    try {
      byte[]   data     = Base64.getDecoder().decode(compressedMessage);
      Inflater inflater = new Inflater();
      inflater.setInput(data);
      outputStream = new ByteArrayOutputStream(data.length);
      byte[] buffer = new byte[2048];
	  int count;
      while (!inflater.finished()) {
        count = inflater.inflate(buffer);
        outputStream.write(buffer, 0, count);
      }

      unCompressedMessage = outputStream.toString();
    } catch (Exception exception) {
      unCompressedMessage = compressedMessage;
      logger.error("Error occurred while decompression of message : " + exception.getMessage());
    } finally {
      try {
        if (outputStream != null) {
          outputStream.close();
        }
      } catch (Exception exception) {
        logger.error("Error occurred while closing stream object : " + exception.getMessage());
      }
    }

    return unCompressedMessage;
  }


    /**
     * As we are using simple JSONObject, we need take responsibility to convert JSON object to Map
     * Provided method will convert JsonObject to Map<String, List<String>>
     *
     * @param object JsonObject having String as a key and JsonArray as value
     *
     * @return return Map<String, List<String>>
     *
     * @throws JSONException throws an exception if anything wrong while converting JsonObject to Map
     * Wrong implementations : 
     * 1. Method can't throw exception at all
     * 2. Not possible of type com.unboundid.util.json.JSONException
     * 3. JSONObject is of type HashMap only; unnecessary conversion
     */
    public static Map<String, List<String>> convertToMap(JSONObject object)
    {
        Map<String, List<String>> map = new HashMap<String, List<String>>();

        try {
            if (object != null) {
                for (Iterator iterator = object.keySet().iterator(); iterator.hasNext(); ) {
                    String key = (String) iterator.next();
                    map.put(key, (List<String>) object.get(key));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }


    /**
     * Default field on which materialized view should get created, are listed here.
     *
     * @param cassandraMVDetails map would be filled with default field list for PIM and view
     *                           after execution of this method
     *
     * @return cassandraMVDetails contains list of default field on which MV should be created.
     */
    public static Map<String, List<String>> getDefaultCassandraMVFields(Map<String, List<String>> cassandraMVDetails)
    {
        if (cassandraMVDetails.isEmpty()) {
            String pdmarticleViewFields[] = {"ParentID",
              "IsFolder",
              "StateID",
              "_IsCreated",
              "_LastWritten",
              "_InsertTime",
              "LanguageID",
              "WorkflowID"};
            String[] mamfile = Stream.of(pdmarticleViewFields).toArray(String[]::new);
            String[] user    = Stream.of(pdmarticleViewFields).toArray(String[]::new);
            String pdmarticleStructureViewFields[] = {"ParentID",
              "IsFolder",
              "StateID",
              "_IsCreated",
              "_LastWritten",
              "_InsertTime",
              "_ExtensionID",
              "LanguageID",
              "WorkflowID"};

            cassandraMVDetails.put(EXPORT_ITEM_TYPE_PDMARTICLE.toLowerCase(), Arrays.asList(pdmarticleViewFields));
            cassandraMVDetails.put(EXPORT_ITEM_TYPE_PDMARTICLESTRUCTURE.toLowerCase(),
                                   Arrays.asList(pdmarticleStructureViewFields));
            cassandraMVDetails.put(EXPORT_ITEM_TYPE_MAMFILE.toLowerCase(), Arrays.asList(mamfile));
            cassandraMVDetails.put(EXPORT_ITEM_TYPE_USER.toLowerCase(), Arrays.asList(user));
        }

        return cassandraMVDetails;
    }
}