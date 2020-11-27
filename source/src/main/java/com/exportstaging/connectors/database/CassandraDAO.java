package com.exportstaging.connectors.database;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.exportstaging.api.exception.ExportStagingException;
import com.exportstaging.common.ExportMiscellaneousUtils;
import com.exportstaging.utils.CassandraListener;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.driver.core.BatchStatement.Type.LOGGED;
import static com.exportstaging.common.ExportMiscellaneousUtils.TAB_DELIMITER;

@Component
public class CassandraDAO {
    @Autowired
    private CassandraConnection conn;
    @Autowired
    private CassandraListener listener;

    @Value("${export.tools.connection.delay}")
    protected int retryDelay;
    @Value("${activemq.subscriber.master}")
    protected String masterSubscriber;

    @Value("${cassandra.statement.batchsize}")
    private int cassandraStatementBatchSize;
    private String cassDownMsg = "Cassandra database unavailable. Waiting for Cassandra database before retrying";
    private String cassUpMsg = "Cassandra connection successful. Resuming waiting operations";
    private String keyspaceTemplate = "keyspace_template";
    protected final Logger logger = LogManager.getLogger("exportstaging");
    private Map<String, Map<String, String>> columnNameWithType     = new HashMap<>();
    private static Map<String, PreparedStatement> prepareStatements = new HashMap<>();

    private static final String CASSANDRA_COLUMN_NOT_IN_METADATA = "is not a column defined in this metadata";
    private static final String CASSANDRA_UNMATCHED_COLUMN_VALUE = "Unmatched column names/values";
    private static final String CASSANDRA_BATCH_EXCEPTION        = "Batch too large";
    private static final String CONST_FORMATE_DATE_TIME          = "yyyy-MM-dd HH:mm:ss";
    private static final String CONST_DEFAULT_DATE_VALUE         = "0000-00-00 00:00:00";

    public CassandraDAO() {
    }

    private boolean validateKeyspace() throws ExportStagingException {
        boolean status;
        try {
            status = this.conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()) != null;
            if (!status) throw new ExportStagingException("Keyspace " + conn.getKeyspace() + " not exists.");
        } catch (NoHostAvailableException e) {
            handleNoHostAvailableException();
            return validateKeyspace();
        }
        return true;
    }

    public boolean isKeyspaceExisting() throws ExportStagingException {
        boolean status = validateKeyspace();
        if (status) {
            String message = "[" + getDatabaseType() + "] Keyspace " + conn.getKeyspace() + " exists.";
            System.out.println(message);
            logger.info(message);
        }
        return status;
    }


    /**
     * Provide the database type, Cassandra or ScyllaDB
     *
     * @return database type, Cassandra or ScyllaDB
     *
     * @throws ExportStagingException if exception occurs while getting the database type then it will be thrown
     */
    public String getDatabaseType() throws ExportStagingException
    {
        return conn.getDatabaseType();
    }


    public boolean checkTable(String tableName) throws ExportStagingException {
        try {
            validateKeyspace();
            return conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getTable(tableName) != null;
        } catch (NoHostAvailableException e) {
            handleNoHostAvailableException();
            return checkTable(tableName);
        }
    }

    public boolean dropMaterializedViews() throws ExportStagingException {
        try {
            Collection<MaterializedViewMetadata> materializedViews = getMaterializedViews();
            for (MaterializedViewMetadata view : materializedViews) {
                String dropViewQuery = "DROP MATERIALIZED VIEW IF EXIST " + keyspaceTemplate + "." + view.getName();
                dbOperations(dropViewQuery);
            }
        } catch (ExportStagingException e) {
            logger.error("[" + getDatabaseType() + "} Exception in dropping Materialized Views: " + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
            return false;
        } catch (Exception e) {
            throw new ExportStagingException(e);
        }
        return true;
    }

    public boolean dropMaterializedViews(String itemType) throws ExportStagingException {
        try {
            ThreadContext.put(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE, itemType);
            Collection<MaterializedViewMetadata> materializedViews = getMaterializedViews();
            for (MaterializedViewMetadata view : materializedViews) {
                if (view.getName().contains(itemType.toLowerCase() + "_")) {
                    String dropViewQuery = "DROP MATERIALIZED VIEW IF EXISTS " + keyspaceTemplate + "." + view.getName();
                    dbOperations(dropViewQuery);
                    logger.info("[" + getDatabaseType() + "]" + view.getName() + " Materialized Views Dropped successfully.");
                }
            }
        } catch (ExportStagingException e) {
            logger.error("[" + getDatabaseType() + "} Exception in dropping Materialized Views." + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
        } catch (Exception e) {
            throw new ExportStagingException(e);
        } finally {
            ThreadContext.remove(ExportMiscellaneousUtils.EXPORT_DATABASE_LOG_ITEM_TYPE);
        }
        return true;
    }

    private Collection<MaterializedViewMetadata> getMaterializedViews() throws ExportStagingException {
        return conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getMaterializedViews();
    }

    public boolean checkColumn(String tableName, String columnName) throws ExportStagingException {
        try {
            return conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getTable(tableName) != null
                    && conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getTable(tableName).getColumn(columnName) != null;
        } catch (NoHostAvailableException e) {
            handleNoHostAvailableException();
            return checkColumn(tableName, columnName);
        }
    }


    public List<String> getColumns(String tableName) throws ExportStagingException {
        List<String> columnList = new ArrayList<>();
        TableMetadata tableMetadata = conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getTable(tableName);
        if (tableMetadata != null) {
            try {
                List<ColumnMetadata> list = tableMetadata.getColumns();
                columnList.addAll(list.stream().map(ColumnMetadata::getName).collect(Collectors.toList()));
            } catch (NoHostAvailableException e) {
                handleNoHostAvailableException();
                return getColumns(tableName);
            } catch (Exception e) {
                throw new ExportStagingException(e);
            }
        }

        return columnList;
    }


    public Map<String, String> getColumnNameWithDataType(String tableName) throws ExportStagingException {
        Map<String, String> columnNameAndType;
        if (getColumnNameWithType().get(tableName) == null) {
            columnNameAndType = new HashMap<>();
            try {
                if (conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getTable(tableName) != null) {
                    List<ColumnMetadata> list = conn.getCluster().getMetadata().getKeyspace(conn.getKeyspace()).getTable(tableName).getColumns();
                    for (ColumnMetadata element : list) {
                        columnNameAndType.put(element.getName(), element.getType().toString());
                    }
                }
            } catch (NoHostAvailableException e) {
                logger.error(cassDownMsg + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
                while (!listener.isHostAvailable()) {
                    sleep();
                }
                logger.info(cassUpMsg);
                return getColumnNameWithDataType(tableName);
            } catch (Exception e) {
                throw new ExportStagingException(e);
            }
            cacheColumnNameWithDataType(tableName, columnNameAndType);
        }
        return new HashMap<>(getColumnNameWithType().get(tableName));
    }

    private void cacheColumnNameWithDataType(String tableName, Map<String, String> columnNameAndType) {
        synchronized (this) {
            columnNameWithType.put(tableName, columnNameAndType);
        }
    }

    private void sleep() {
        try {
            Thread.sleep(retryDelay);
        } catch (InterruptedException e) {
            logger.error("Interrupted Exception" + e.getMessage() + TAB_DELIMITER + Arrays.toString(e.getStackTrace()));
            Thread.currentThread().interrupt();
        }
    }

    public ResultSet dbOperations(String query) throws ExportStagingException {
        try {
            PreparedStatement prepared = conn.getSession().prepare(query.replace(keyspaceTemplate, conn.getKeyspace()));
            BoundStatement bound = (BoundStatement) prepared.bind().setReadTimeoutMillis(ExportMiscellaneousUtils.CONST_READ_TIMEOUT_IN_MILISEC);
            return conn.getSession().execute(bound);
        } catch (NoHostAvailableException e) {
            handleNoHostAvailableException();
            return dbOperations(query);
        } catch (Exception e) {
            throw new ExportStagingException(e);
        }
    }

    private void handleNoHostAvailableException() {
        logger.error("[" + masterSubscriber + "] " + cassDownMsg);
        while (!listener.isHostAvailable()) {
            sleep();
        }
        logger.info("[" + masterSubscriber + "] " + cassUpMsg);
        System.out.println("[" + masterSubscriber + "] " + cassUpMsg);
    }

    public boolean bulkInsert(List<String> queries) throws ExportStagingException {
        Iterable<List<String>> partition = Iterables.partition(queries, 10);
        for (List<String> batch : partition) {
            insertBatch(batch);
        }
        return true;
    }


    /**
     * Method will be responsible for insertion/updation of object's reference or subtable data into Cassandra
     *
     * @param tableName               String name of Cassandra table
     * @param objectFieldValueMapping Map of field name and its value
     * @param columnNameWithDataType  Map of field name and its datatype
     * @param bindStatement           Set of BoundStatement
     * @throws ExportStagingException exception object if exception occurs
     */
    public void processReferenceOrSubtableData(
            String tableName,
            Map<String, Object> objectFieldValueMapping,
            Map<String, String> columnNameWithDataType,
            Set<BoundStatement> bindStatement) throws ExportStagingException {
        Set<String> columnNames = objectFieldValueMapping.keySet();
        PreparedStatement statement = getStatement(tableName, String.join(",", objectFieldValueMapping.keySet()));
        BoundStatement boundStatement = statement.bind();
        try {
            for (String columnName : columnNames) {
                String columnDataType = columnNameWithDataType.get(columnName);
                Object columnValue = objectFieldValueMapping.get(columnName);
                prepareBoundStatement(boundStatement, columnName, columnDataType, columnValue);
            }
        } catch (ExportStagingException exception) {
            if (exception.getMessage().contains(CASSANDRA_COLUMN_NOT_IN_METADATA)) {
                prepareStatements.remove(tableName);
                processReferenceOrSubtableData(tableName, objectFieldValueMapping, columnNameWithDataType, bindStatement);
            } else {
                throw exception;
            }
        }
        bindStatement.add(boundStatement);
    }


    /**
     * Execution of batch statement will be done here
     * If execution of batching failed single statement will be executed
     *
     * @param bindStatement Set of BoundStatement
     * @return true if execution of statement is success otherwise false
     * @throws ExportStagingException exception object if exception occurs
     */
    public Boolean executeBatch(Set<BoundStatement> bindStatement) throws ExportStagingException {
        BatchStatement batch = new BatchStatement(LOGGED);

        try {
            Iterable<List<BoundStatement>> lists = Iterables.partition(bindStatement, cassandraStatementBatchSize);
            for (List<BoundStatement> list : lists) {
                batch.addAll(list);
                try {
                    conn.getSession().execute(batch);
                } catch (Exception exception) {
                    if (exception.getMessage().contains(CASSANDRA_BATCH_EXCEPTION)) {
                        logger.info(
                                "[" + getDatabaseType() + "] Batch is too big for bulk insert in all languages to resolve this"
                                        + " inserting data in one by one language.");
                        //Only the current batch statements should be processed
                        for (Statement boundStatement : batch.getStatements()) {
                            conn.getSession().execute(boundStatement);
                        }
                    } else {
                        throw new ExportStagingException(exception);
                    }
                } finally {
                    batch.clear();
                }
            }
            return true;
        } catch (Exception exception) {
            throw new ExportStagingException(exception);
        }
    }


    /**
     * Method will be responsible to insert/update data into cassanda for object
     *
     * @param tableName              String name of table
     * @param attributeValueMapping  Map of attribute and its values
     * @param columnNameWithDataType Map of columnName/attribute name with its type
     * @param bindStatement          Set of BoundStatement
     * @return true if execution of statement is success otherwise false
     * @throws ExportStagingException exception object if exception occurs
     */
    public boolean processItemData(
            String tableName, Map<String, Object> attributeValueMapping,
            Map<String, String> columnNameWithDataType,
            Set<BoundStatement> bindStatement) throws Exception {
        Set<String> columnNames = attributeValueMapping.keySet();
        PreparedStatement statement = getItemStatement(tableName, String.join(",", columnNames));
        BoundStatement boundStatement = statement.bind();
        try {
            for (String columnName : columnNames) {
                String columnDataType = columnNameWithDataType.get(columnName);
                Object columnValue = attributeValueMapping.get(columnName);
                prepareBoundStatement(boundStatement, columnName, columnDataType, columnValue);
            }
        } catch (ExportStagingException exception) {
            if (exception.getMessage().contains(CASSANDRA_COLUMN_NOT_IN_METADATA)) {
                prepareStatements.remove(tableName);
                processItemData(tableName, attributeValueMapping, columnNameWithDataType, bindStatement);
            } else {
                throw exception;
            }
        }
        bindStatement.add(boundStatement);

        return true;
    }


    /**
     * Bind column with provided field data
     * Column Name and its values bind according to data type
     *
     * @param boundStatement BoundStatement where we need to bind the values
     * @param fieldName String name of field
     * @param fieldType String type of field
     * @param columnValues Object column Values
     *
     * @throws ExportStagingException exception object if exception occurs
     */
    private void prepareBoundStatement(BoundStatement boundStatement, String fieldName, String fieldType, Object columnValues) throws ExportStagingException {
        if (StringUtils.isEmpty(fieldType)) {
            fieldType = "text";
        }
        try {
            switch (fieldType) {
                case "int":
                    String intFieldValue = getValueFromObject(columnValues);
                    intFieldValue = (StringUtils.isEmpty(intFieldValue) || intFieldValue.equalsIgnoreCase("null")) ? "0" : intFieldValue;
                    boundStatement.setInt(fieldName, Integer.parseInt(intFieldValue));
                    break;
                case "bigint":
                    String bigintFieldValue = getValueFromObject(columnValues);
                    bigintFieldValue = (StringUtils.isEmpty(bigintFieldValue) || bigintFieldValue.equalsIgnoreCase("null")) ? "0" : bigintFieldValue;
                    boundStatement.setLong(fieldName, Long.parseLong(bigintFieldValue));
                    break;
                case "float":
                    String floatFieldValue = getValueFromObject(columnValues);
                    floatFieldValue = (StringUtils.isEmpty(floatFieldValue) || floatFieldValue.equalsIgnoreCase("null")) ? "0" : floatFieldValue;
                    boundStatement.setFloat(fieldName, Float.parseFloat(floatFieldValue));
                    break;
                case "timestamp":
                    boundStatement.setTimestamp(fieldName, getFormattedDate(columnValues));
                    break;
                default:
                    boundStatement.setString(fieldName, String.valueOf(columnValues));
                    break;
            }
        } catch (Exception e) {
            //System.out.println("Field Name: " + fieldName + " Type " + fieldType + " " + e.getMessage());
            throw new ExportStagingException(e.getMessage());
        }
    }


    /**
     * get String conversion of columnValues
     *
     * @param columnValue String column values
     * @return String value after conversion from different type
     */
    private String getValueFromObject(Object columnValue) {
        return String.valueOf(columnValue);
    }


    /**
     * Date format will be return as per the provided date information
     *
     * @param providedDate Object provided date format
     *
     * @return date after parsing
     *
     * @throws ParseException exception object if exception occurs
     */
    private Date getFormattedDate(Object providedDate) throws ParseException {
        Date parseDate;
        String dateValue = getValueFromObject(providedDate);
        try {
            if (StringUtils.isEmpty(dateValue) || dateValue.equalsIgnoreCase("null")) {
                dateValue = CONST_DEFAULT_DATE_VALUE;
                parseDate = new SimpleDateFormat(CONST_FORMATE_DATE_TIME).parse(dateValue);
            } else if (dateValue.contains("now")) {
                DateFormat dateFormat = new SimpleDateFormat(CONST_FORMATE_DATE_TIME);
                 parseDate = new Date();
                dateFormat.format(parseDate);
            } else {
                parseDate = new SimpleDateFormat(CONST_FORMATE_DATE_TIME).parse(dateValue);
            }
        }catch (ParseException exception) {
            if(!dateValue.contains(":")){
                dateValue = dateValue + " 00:00:00";
            }
            parseDate = new SimpleDateFormat(CONST_FORMATE_DATE_TIME).parse(dateValue);
        }
        return parseDate;
    }


    /**
     * Provides statement for reference handling
     *
     * @param tableName   String name of table as of now its useful for reference and subtable
     * @param columnNames String all columns in the form of String with ',' separated
     * @return PreparedStatement for reference and subtable
     * @throws ExportStagingException exception object if exception occurs
     */
    public PreparedStatement getStatement(String tableName, String columnNames) throws ExportStagingException {
        return prepareStatement(tableName, columnNames);
    }


    /**
     * Provides statement for reference handling
     *
     * @param tableName   String name of table as of now its useful for reference and subtable
     * @param columnNames String all columns in the form of String with ',' separated
     * @return PreparedStatement for reference and subtable
     * @throws ExportStagingException exception object if exception occurs
     */
    public PreparedStatement getItemStatement(String tableName, String columnNames) throws ExportStagingException {
        return prepareStatement(tableName, columnNames);
    }


    /**
     * PreparedStatement will be prepare as per the cassandra table name
     * for each table separate PreparedStatement will be prepared and cache for further used
     * Only values would be bind to this statement, no new PreparedStatement will be generated
     *
     * @param tableName   String name of table
     * @param columnNames String All column with separated by ','
     * @return PreparedStatement for further used
     * @throws ExportStagingException exception object if exception occurs
     */
    public PreparedStatement prepareStatement(String tableName, String columnNames) throws ExportStagingException {
        if (prepareStatements.get(tableName) == null) {
            String queryFormat = "insert into " + conn.getKeyspace() + "." + tableName + " (" + columnNames + ") values(";
            long columnCount = columnNames.chars().filter(ch -> ch == ',').count();
            String bindExpression = "";
            for (int counter = 0; counter <= columnCount; counter++) {
                bindExpression = bindExpression + "?, ";
            }
            bindExpression = StringUtils.substring(bindExpression, 0, bindExpression.length() - 2);
            queryFormat = queryFormat + bindExpression + ")";
            PreparedStatement prepared = null;
            try {
                prepared = conn.getSession().prepare(queryFormat);
            } catch (Exception e) {
                String message = e.getMessage();
                if (message.contains(CASSANDRA_UNMATCHED_COLUMN_VALUE)) {
                    prepareStatements.remove(tableName);
                    prepareStatement(tableName, columnNames);
                }
                throw new ExportStagingException(e);
            }
            prepareStatements.put(tableName, prepared);

        }

        return prepareStatements.get(tableName);
    }


    private boolean insertBatch(List<String> queries) throws ExportStagingException {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.setReadTimeoutMillis(ExportMiscellaneousUtils.CONST_READ_TIMEOUT_IN_MILISEC);
        for (String query : queries) {
            SimpleStatement statement = new SimpleStatement(query.replace(keyspaceTemplate, conn.getKeyspace()));
            statement.setReadTimeoutMillis(ExportMiscellaneousUtils.CONST_READ_TIMEOUT_IN_MILISEC);
            batch.add(statement);
        }
        try {
            conn.getSession().execute(batch);
        } catch (NoHostAvailableException e) {
            handleNoHostAvailableException();
            return insertBatch(queries);
        } catch (InvalidQueryException e) {
            if ("Batch too large".equalsIgnoreCase(e.getMessage())) {
                logger.info("[" + getDatabaseType() + "] Batch is too big for bulk insert in all languages to resolve this inserting data in one by one language.");
                for (String query : queries) {
                    dbOperations(query);
                }
            } else {
                throw new ExportStagingException(e);
            }
        } catch (Exception e) {
            throw new ExportStagingException(e);
        }
        return true;
    }

    public void dynamicAddColumnNameWithDataType(String tableName, String columnName, String dataType) {
        if (getColumnNameWithType().get(tableName) != null) {
            synchronized (this) {
                getColumnNameWithType().get(tableName).put(columnName, dataType);
            }
        }
    }

    public void removeColumnNameWithDataType(String tableName, String columnName) {
        if (getColumnNameWithType().get(tableName) != null) {
            synchronized (this) {
                getColumnNameWithType().get(tableName).remove(columnName);
            }
        }
    }

    private Map<String, Map<String, String>> getColumnNameWithType() {
        synchronized (this) {
            return columnNameWithType;
        }
    }

    public void closeConnection() {
        conn.closeConnection();
    }
}
