#!/bin/sh -
#======================================================================================================================
# vim: softtabstop=4 shiftwidth=4 expandtab fenc=utf-8 spell spelllang=en cc=120
#======================================================================================================================
#
#          FILE: ExportExecutor.sh
#
#   DESCRIPTION: ExportExecutor startup/shutdown script
#
#          BUGS: https://contentserv.atlassian.net/issues
#
#     COPYRIGHT: (c) 2017 by Contentserv GmbH
#
#        AUTHOR: Kirill Peskov
#       LICENSE: Proprietary
#  ORGANIZATION: Contentserv GmbH (contentserv.com)
#       CREATED: 13/10/2017 14:30 CET
#======================================================================================================================

# ExportExecutor script truth values
EE_TRUE=1
EE_FALSE=0

# Enable debug if global variable is set
[ $CS_ENABLE_DEBUG ] && _ECHO_DEBUG=$EE_TRUE || _ECHO_DEBUG=$EE_FALSE

# From here and onwards,
# treat unset variables as an error
set -o nounset

__ScriptVersion="0.1.4"
__ScriptName="ExportExecutor.sh"

# Default sleep time used when waiting for daemons to start, restart and checking for these running
__DEFAULT_SLEEP=5

# Some daemons need more time on the first start, so the script will make several attempts 
# to connect to them, sleeping __DEFAULT_SLEEP seconds between each try. Total number of attempts is limited
# by this variable
__DEFAULT_MAX_ATTEMPTS=10

# Autodetect script name and location, calcualte and set some defaults
__ScriptFullName="$0"
__ScriptArgs="$*"

__ScriptFullPath=$(realpath "$__ScriptFullName")
__ScriptDir=$(dirname "$__ScriptFullPath")
__ExportExecutorScript="admin/core/extensions/exportstaging/java/dist"
__ExportExecutorSubdir="admin.local/lib/exportstaging"
#__ExportExecutorSubdir="admin.local/lib/exportstaging"
_CS_WEBROOT=${__ScriptDir%$__ExportExecutorScript}
_CS_PROJECTS=""

_CS_USER="www-data"
_CS_GROUP="www-data"

_CS_CORE_PROPERTIES_PATH_SUFFIX="admin/core/extensions/exportstaging/java/dist/properties"
_CS_PROJECT_PROPERTIES_PATH_SUFFIX="data/exportstaging/config/properties"

_CS_ACTIVEMQ_ROOT="${_CS_WEBROOT}/admin.local/lib/activemq/linux"
_CS_ACTIVEMQ_LOG="/var/log/contentserv-cs/cs-activemq.log"
_CS_ACTIVEMQ_COMMAND="sh ${_CS_ACTIVEMQ_ROOT}/bin/activemq start"
_CS_ACTIVEMQ_PID="${_CS_ACTIVEMQ_ROOT}/data/activemq.pid"
# ActiveMQ global defaults (not project-specific)
_CS_ACTIVEMQ_JOLOKIA_PORT=8161
_CS_ACTIVEMQ_HOST=localhost
_CS_ACTIVEMQ_USER=admin
_CS_ACTIVEMQ_PASS=admin

# Cassandra global defaults (not project-specific)
_CS_CASSANDRA_LOG="/var/log/contentserv-cs/cs-cassandra.log"
_CS_CASSANDRA_COMMAND="sh ${_CS_WEBROOT}/admin.local/lib/cassandra/bin/cassandra"
_CS_CASSANDRA_PID="${_CS_WEBROOT}/admin.local/lib/cassandra/data/cassandra.pid"
_CS_CASSANDRA_DATADIR="${_CS_WEBROOT}/admin.local/lib/cassandra/data"
# Cassandra pre-shutdown command (compact/flush)
_CS_CASSANDRA_PRE_SHUTDOWN="sudo -u ${_CS_USER} -H sh ${_CS_WEBROOT}/admin.local/lib/cassandra/bin/nodetool compact && \
                            sudo -u ${_CS_USER} -H sh ${_CS_WEBROOT}/admin.local/lib/cassandra/bin/nodetool flush"
# Replacing bundled CQLSH with more generic one from PyPI packets (should be pre-installed) 
_CS_CQLSH="sh ${_CS_WEBROOT}admin.local/lib/cassandra/bin/cqlsh"
#_CS_CQLSH="/usr/local/bin/cqlsh"
# Default cql_version for cqlsh probe 
_CS_CQL_VER="3.4.4"
_CS_CASSANDRA_HOST=localhost
_CS_CASSANDRA_PORT=9042
# Vital tweak for 'own' cqlsh
CQLSH_NO_BUNDLED=TRUE
export CQLSH_NO_BUNDLED

# ElasticSearch global defaults (not project-specific)
_CS_ELASTIC_LOG="/var/log/contentserv-cs/cs-elasticsearch.log"
_CS_ELASTIC_COMMAND="bash ${_CS_WEBROOT}/admin.local/lib/elasticsearch/bin/elasticsearch -d"
_CS_ELASTIC_PID="${_CS_WEBROOT}/admin.local/lib/elasticsearch/data/elasticsearch.pid"
_CS_ELASTIC_DATADIR="${_CS_WEBROOT}/admin.local/lib/elasticsearch/data"
_CS_ELASTIC_HOST=localhost
_CS_ELASTIC_PORT=9200

_CS_EE_JAR="ExportExecutor.jar"
_CS_EE_JAVAOPTS="-Xms512M -Xmx1024M"
_CS_EE_COMMAND="java $_CS_EE_JAVAOPTS -jar ${_CS_WEBROOT}/${__ExportExecutorSubdir}/${_CS_EE_JAR}"
_CS_EE_PID=""

#echo $_CS_EE_COMMAND
#_CS_USER="apache"
#_CS_GROUP="apache"


#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __detect_color_support
#   DESCRIPTION:  Try to detect color support.
#----------------------------------------------------------------------------------------------------------------------
_COLORS=${BS_COLORS:-$(tput colors 2>/dev/null || echo 0)}
__detect_color_support() {
    if [ $? -eq 0 ] && [ "$_COLORS" -gt 2 ]; then
        RC="\033[1;31m"
        GC="\033[1;32m"
        BC="\033[1;34m"
        YC="\033[1;33m"
        EC="\033[0m"
    else
        RC=""
        GC=""
        BC=""
        YC=""
        EC=""
    fi
}
__detect_color_support

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  echoerr
#   DESCRIPTION:  Echo errors to stderr.
#----------------------------------------------------------------------------------------------------------------------
echoerror() {
    printf "${RC} * ERROR${EC}: %s\n" "$@" 1>&2;
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  echoinfo
#   DESCRIPTION:  Echo information to stdout.
#----------------------------------------------------------------------------------------------------------------------
echoinfo() {
    printf "${GC} *  INFO${EC}: %s\n" "$@";
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  echowarn
#   DESCRIPTION:  Echo warning informations to stdout.
#----------------------------------------------------------------------------------------------------------------------
echowarn() {
    printf "${YC} *  WARN${EC}: %s\n" "$@";
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  echodebug
#   DESCRIPTION:  Echo debug information to stdout.
#----------------------------------------------------------------------------------------------------------------------
echodebug() {
    if [ "$_ECHO_DEBUG" -eq $EE_TRUE ]; then
        printf "${BC} * DEBUG${EC}: %s\n" "$@";
    fi
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __detect_projects
#   DESCRIPTION:  Try to detect CS projects. Project considered 'active' if config.php is in place
#                 _CS_WEBROOT should be already detected
#----------------------------------------------------------------------------------------------------------------------
__detect_projects() {
    if [ $_CS_WEBROOT = "" ];
    then
        echoerror "Unable to detect CS WebRoot"
        exit 1
    fi
    _CS_PROJECT_DIRS=$(find $_CS_WEBROOT -name "config.php" \
                     | grep -v "${_CS_WEBROOT}admin" \
                     | sed -n 's/\(.*\)\/data\/config\.php/\1/p')
    for _CS_PDIR in $_CS_PROJECT_DIRS
    do
        echodebug "Stripping $_CS_PDIR"
        _CS_PROJECT=${_CS_PDIR#$_CS_WEBROOT}
        _CS_PROJECTS="${_CS_PROJECTS}$_CS_PROJECT "
    done
}

__detect_projects


#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __start_activemq
#   DESCRIPTION:  Start local ActiveMQ (if not already running)
#----------------------------------------------------------------------------------------------------------------------
__start_activemq() {

    if ! (ps ax | grep java | grep -q activemq) ;
    then
        echoinfo "Local ActiveMQ is not running, starting..."
        sudo -u $_CS_USER -H $_CS_ACTIVEMQ_COMMAND >>$_CS_ACTIVEMQ_LOG 2>>$_CS_ACTIVEMQ_LOG
        echodebug "Changing the log/pid files owner to ${_CS_USER}:${_CS_GROUP}"
        chown ${_CS_USER}:${_CS_GROUP} $_CS_ACTIVEMQ_LOG
        chown ${_CS_USER}:${_CS_GROUP} $_CS_ACTIVEMQ_PID
    else
        echoinfo "Local ActiveMQ is already started"
    fi

    # ActiveMQ health check

    HC_COUNTER=$__DEFAULT_MAX_ATTEMPTS

    while ! (curl -s -u "$_CS_ACTIVEMQ_USER":"$_CS_ACTIVEMQ_PASS" http://${_CS_ACTIVEMQ_HOST}:${_CS_ACTIVEMQ_JOLOKIA_PORT}/api/jolokia | grep -q "\"status\":200" );
    do
        HC_COUNTER=$((HC_COUNTER-1))
        if [ "$HC_COUNTER" -le 0 ];
        then
            echoerror "Failed to start localhost ActiveMQ, aborting"
            exit 1
        fi
        echoinfo "Waiting for ActiveMQ to come up"
        echodebug "Sleeping "$__DEFAULT_SLEEP" seconds, "$HC_COUNTER" attempts left"
        sleep $__DEFAULT_SLEEP
    done

    echoinfo "Local ActiveMQ health check passed, API login OK"

}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __start_cassandra
#   DESCRIPTION:  Start local Cassandra (if not already running)
#----------------------------------------------------------------------------------------------------------------------
__start_cassandra() {

    if ! [ -d "$_CS_CASSANDRA_DATADIR" ] ;
    then
        echodebug "Cassandra local datadir not exists, creating"
        if ! (sudo -u $_CS_USER -H mkdir -m 775 -p "$_CS_CASSANDRA_DATADIR")
        then
            echoerror "Unable to create Cassandra datadir, aborting"
            exit 1
        fi
    fi


    if ! (ps ax | grep java | grep -q "org.apache.cassandra.service.CassandraDaemon") ;
    then
        echoinfo "Local Cassandra is not running, starting..."
        if ! (sudo -u $_CS_USER -H $_CS_CASSANDRA_COMMAND >>$_CS_CASSANDRA_LOG 2>>$_CS_CASSANDRA_LOG);
        then
            echoerror "Unable to start local Cassandra, aborting..."
            exit 1
        fi
    
        echodebug "Changing the log file owner to ${_CS_USER}:${_CS_GROUP}"
        chown ${_CS_USER}:${_CS_GROUP} $_CS_CASSANDRA_LOG
    
        echodebug "Detecting Cassandra PID..."
        ps ax | grep "org.apache.cassandra.service.CassandraDaemon" | grep -v "grep" | awk '{print $1}' > $_CS_CASSANDRA_PID
        echodebug "Cassandra PID: "$(cat $_CS_CASSANDRA_PID)
        chown ${_CS_USER}:${_CS_GROUP} $_CS_CASSANDRA_PID
    else
        echoinfo "Local Cassandra process is already running"
    fi

    HC_COUNTER=$__DEFAULT_MAX_ATTEMPTS
    
    while ! ($_CS_CQLSH --cqlversion=${_CS_CQL_VER} -e "SHOW host" $_CS_CASSANDRA_HOST $_CS_CASSANDRA_PORT | grep -q  "Connected to" );
#    while ! ($_CS_CQLSH --cqlversion=${_CS_CQL_VER} -e "SHOW host" $_CS_CASSANDRA_HOST $_CS_CASSANDRA_PORT 2>>/dev/null | grep -q  "Connected to" );
    do
        HC_COUNTER=$((HC_COUNTER-1))
        if [ "$HC_COUNTER" -le 0 ];
        then
            echoerror "Failed to start local Cassandra, aborting"
            exit 1
        fi
        echoinfo "Waiting for Cassandra to come up"
        echodebug "Sleeping "$__DEFAULT_SLEEP" seconds, "$HC_COUNTER" attempts left"
        sleep $__DEFAULT_SLEEP
    done

    echoinfo "Local Cassandra health check passed, CQLSH connection OK"
    
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __start_elasticsearch
#   DESCRIPTION:  Start local ElasticSearch (if not already running)
#----------------------------------------------------------------------------------------------------------------------
__start_elasticsearch() {

    if ! [ -d "$_CS_ELASTIC_DATADIR" ] ;
    then
        echodebug "ElasticSearch local datadir not exists, creating"
        if ! (sudo -u $_CS_USER -H mkdir -m 775 -p "$_CS_ELASTIC_DATADIR")
        then
            echoerror "Unable to create ElasticSearch datadir, aborting"
            exit 1
        fi
    fi


    if ! (ps ax | grep java | grep -q "org.elasticsearch.bootstrap") ;
    then
        echoinfo "Local ElasticSearch is not running, starting..."
        if ! (sudo -u $_CS_USER -H $_CS_ELASTIC_COMMAND >>$_CS_ELASTIC_LOG 2>>$_CS_ELASTIC_LOG);
        then
            echoerror "Unable to start local ElasticSearch, exiting..."
            exit 1
        fi
        echodebug "Detecting ElasticSearch PID..."
        ps ax | grep "org.elasticsearch.bootstrap" | grep -v "grep" | awk '{print $1}' > $_CS_ELASTIC_PID
        echodebug "ElasticSearch PID: "$(cat $_CS_ELASTIC_PID)
    else
        echoinfo "Local ElasticSearch process already running"
    fi

    HC_COUNTER=$__DEFAULT_MAX_ATTEMPTS

    while ! (curl -s http://${_CS_ELASTIC_HOST}:${_CS_ELASTIC_PORT}/_cluster/health 2>>/dev/null | grep -e "\"status\":\"green\"") ;
    do
        HC_COUNTER=$((HC_COUNTER-1))
        if [ "$HC_COUNTER" -le 0 ];
        then
            echoerror "Failed to start local ElasticSearch, aborting"
            exit 1
        fi
        echoinfo "Waiting for ElasticSearch to come up"
        echodebug "Sleeping "$__DEFAULT_SLEEP" seconds, "$HC_COUNTER" attempts left"
        sleep $__DEFAULT_SLEEP
    done

    echoinfo "Local ElasticSearch health check passed, cluster status is green"

}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __start_all_projects
#   DESCRIPTION:  Start all previously detected projects
#----------------------------------------------------------------------------------------------------------------------
__start_all_projects() {
    echoinfo "Starting all the projects"
    
    echoinfo "First check local ActiveMQ and start if needed"
    __start_activemq
    
    for _CS_PROJECT in $_CS_PROJECTS 
    do
        echoinfo "Starting $_CS_PROJECT"

        if ! [ -d ${_CS_WEBROOT}${_CS_PROJECT}/${_CS_PROJECT_PROPERTIES_PATH_SUFFIX} ];
        then
            echowarn "Project ${_CS_PROJECT} does not have configuration directory for Java processes, skipping"
            continue
        fi

        echodebug "Detecting ActiveMQ settings"
        _CS_ACTIVEMQ_PROJECT_PROPS=${_CS_WEBROOT}${_CS_PROJECT}/${_CS_PROJECT_PROPERTIES_PATH_SUFFIX}/activemq.properties
        _CS_PROJECT_ACTIVEMQ_HOST=$(cat $_CS_ACTIVEMQ_PROJECT_PROPS | sed -n 's/\s*activemq\.url\.host\s*=\s*\(.*\)/\1/p')
        _CS_PROJECT_ACTIVEMQ_USER=$(cat $_CS_ACTIVEMQ_PROJECT_PROPS | sed -n 's/\s*activemq\.username\s*=\s*\(.*\)/\1/p')
        _CS_PROJECT_ACTIVEMQ_PASS=$(cat $_CS_ACTIVEMQ_PROJECT_PROPS | sed -n 's/\s*activemq\.password\s*=\s*\(.*\)/\1/p')
        echodebug "ActiveMQ host: $_CS_PROJECT_ACTIVEMQ_HOST, username: $_CS_PROJECT_ACTIVEMQ_USER, password: $_CS_PROJECT_ACTIVEMQ_PASS"

        if (curl -s -u "$_CS_PROJECT_ACTIVEMQ_USER":"$_CS_PROJECT_ACTIVEMQ_PASS" http://${_CS_PROJECT_ACTIVEMQ_HOST}:${_CS_ACTIVEMQ_JOLOKIA_PORT}/api/jolokia | grep -q "\"status\":200" );
        then
            echodebug "ActiveMQ API login OK"
        else
            echowarn "ActiveMQ is not available for project: $_CS_PROJECT, skipping"
            continue
        fi

        # Cassandra section --------------------------------------------------------------

        echodebug "Detecting Cassandra settings"
        _CS_CASSANDRA_PROJECT_PROPS=${_CS_WEBROOT}${_CS_PROJECT}/${_CS_PROJECT_PROPERTIES_PATH_SUFFIX}/cassandra.properties
        _CS_PROJECT_CASSANDRA_HOST=$(cat $_CS_CASSANDRA_PROJECT_PROPS | sed -n 's/\s*cassandra\.connection\.url\s*=\s*\(.*\)/\1/p')
        echodebug "Cassandra host: $_CS_PROJECT_CASSANDRA_HOST"
#        _CS_PROJECT_CASSANDRA_PORT=$(cat $_CS_CASSANDRA_PROJECT_PROPS | sed -n 's/\s*cassandra\.connection\.port\s*=\s*\(.*\)/\1/p')
        _CS_PROJECT_CASSANDRA_PORT=9042
        echodebug "Cassandra port: $_CS_PROJECT_CASSANDRA_PORT"

        if [ "$_CS_PROJECT_CASSANDRA_HOST" = "localhost" ];
        then
            echoinfo "Setup for local Cassandra detected"
            __start_cassandra
        else
            echoinfo "Remote Cassandra"
        fi

        echoinfo "Probing Cassandra on host: "$_CS_PROJECT_CASSANDRA_HOST", port: "$_CS_PROJECT_CASSANDRA_PORT

        if $_CS_CQLSH --cqlversion=${_CS_CQL_VER} -e "SHOW host" $_CS_PROJECT_CASSANDRA_HOST $_CS_PROJECT_CASSANDRA_PORT 2>&1 | grep -q "Connected to" ;
        then
            echoinfo "Successfully connected"
        else
            echodebug "Could be cql_version mismatch, trying to enforce the version supported by the server"
            REMOTE_CQL_VER=$($_CS_CQLSH --cqlversion=${_CS_CQL_VER} -e "SHOW host" $_CS_PROJECT_CASSANDRA_HOST \
                             $_CS_PROJECT_CASSANDRA_PORT 2>&1 |  sed -e 's/^.*Supported versions:\s.*\[u\(.*\)\].*/\1/g')
            if [ "$REMOTE_CQL_VER" = "" ];
            then
                echowarn "Unable to connect to detect server cql_version, skipping project $_CS_PROJECT"
                continue
            fi
          
            TEMPV="${REMOTE_CQL_VER%\'}"
            REMOTE_CQL_VER="${TEMPV#\'}"
            echodebug "CQL version supported by the server: "$REMOTE_CQL_VER
          
            if $_CS_CQLSH --cqlversion=${REMOTE_CQL_VER} -e "SHOW host" $_CS_PROJECT_CASSANDRA_HOST $_CS_PROJECT_CASSANDRA_PORT 2>&1 | grep -q "Connected to" ;
            then
                echoinfo "Successfully connected with cql_version="$REMOTE_CQL_VER
            else
                echowarn "Unable to connect, skipping project $_CS_PROJECT"
                continue
            fi
        fi

        # ElasticSearch section --------------------------------------------------------------

        echodebug "Detecting ElasticSearch settings"
        _CS_ELASTIC_PROJECT_PROPS=${_CS_WEBROOT}${_CS_PROJECT}/${_CS_PROJECT_PROPERTIES_PATH_SUFFIX}/elasticsearch.properties
        _CS_PROJECT_ELASTIC_HOST=$(cat $_CS_ELASTIC_PROJECT_PROPS | sed -n 's/\s*elasticsearch\.address\s*=\s*\(.*\)/\1/p')
        echodebug "ElasticSearch host: $_CS_PROJECT_ELASTIC_HOST"
        
        _CS_PROJECT_ELASTIC_PORT=$(cat $_CS_ELASTIC_PROJECT_PROPS | sed -n 's/\s*elasticsearch\.http\.port\s*=\s*\(.*\)/\1/p')
        echodebug "ElasticSearch http port: $_CS_PROJECT_ELASTIC_PORT"

        if [ "$_CS_PROJECT_ELASTIC_HOST" = "localhost" ];
        then
            echoinfo "Setup for local ElasticSearch detected"
            __start_elasticsearch
        else
            echoinfo "Remote ElasticSearch"
        fi

        echoinfo "Probing ElasticSearch on host:"$_CS_PROJECT_ELASTIC_HOST", port: "$_CS_PROJECT_ELASTIC_PORT
	if (curl -s http://${_CS_PROJECT_ELASTIC_HOST}:${_CS_PROJECT_ELASTIC_PORT}/_cluster/health | grep -e "\"status\":\"green\"") ;
        then
            echoinfo "Successfully connected"
        else
            echowarn "Unable to connect, skipping project $_CS_PROJECT"
            continue
        fi

        # ExportExecutor section --------------------------------------------------------------

        if ! (ps ax | grep java | grep "ExportExecutor" | grep -q "$_CS_PROJECT") ;
        then
            echoinfo "Starting ExportExecutor for $_CS_PROJECT"
            _CS_EE_PID="${_CS_WEBROOT}/${_CS_PROJECT}/data/exportstaging/cs-ee.pid"
            if (sudo -u $_CS_USER -H nohup $_CS_EE_COMMAND $_CS_PROJECT >>/dev/null 2>>/dev/null &) ;
            then
                sleep $__DEFAULT_SLEEP
                echodebug "Changing the file owner to ${_CS_USER}:${_CS_GROUP}"
                echodebug "Started, detecting the PID"
                sleep $__DEFAULT_SLEEP
                ps ax | grep java | grep "ExportExecutor" | grep "$_CS_PROJECT" | grep -v "grep" | grep -v "sudo" | awk '{print $1}' >$_CS_EE_PID
                chown ${_CS_USER}:${_CS_GROUP} $_CS_EE_PID
            else
                echoerror "Unable to start ExportExecutor JAR"
                exit 1
            fi
            
        else
            echoinfo "ExportExecutor for $_CS_PROJECT is already running"
        fi

    done
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __stop_ee
#   DESCRIPTION:  Stop (gracefully) locally running ExportExecutor, project name as an argument
#----------------------------------------------------------------------------------------------------------------------
__stop_ee() {
    PRJ="$@"
    echoinfo "Shutdown local processes for: $PRJ"
    echodebug "First, shutdown ExportExecutor JAR"
    echodebug "Detecting ExportExecutor PID..."
    _CS_EE_PID="${_CS_WEBROOT}/${PRJ}/data/exportstaging/cs-ee.pid"
    _CS_EE_AUTODETECT_PID=$(ps ax | grep java | grep "ExportExecutor" | grep "$PRJ" | grep -v "grep" | grep -v "sudo" | awk '{print $1}')
    if [ "$_CS_EE_AUTODETECT_PID" = "" ];
    then
        echodebug "No running ExportExecutor detected for $PRJ"
    else
        echoinfo "Shutdown PID: "$_CS_EE_AUTODETECT_PID
        kill $_CS_EE_AUTODETECT_PID
        rm $_CS_EE_PID || true
    fi
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __stop_activemq
#   DESCRIPTION:  Stop (gracefully) locally running ActiveMQ,
#----------------------------------------------------------------------------------------------------------------------
__stop_activemq() {

    echodebug "Detecting ActiveMQ PID..."
    
    _CS_ACTIVEMQ_AUTODETECT_PID=$(ps ax | grep java | grep "activemq" | grep -v "grep" | grep -v "sudo" | awk '{print $1}')
    
    if [ "$_CS_ACTIVEMQ_AUTODETECT_PID" = "" ];
    then
        echodebug "No running ActiveMQ detected"
    else
        echoinfo "Shutdown PID: "$_CS_ACTIVEMQ_AUTODETECT_PID
        kill $_CS_ACTIVEMQ_AUTODETECT_PID
        rm $_CS_ACTIVEMQ_PID || true
    fi
}


#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __stop_all_projects
#   DESCRIPTION:  Stop (gracefully) all locally running Java processes for all projects
#----------------------------------------------------------------------------------------------------------------------
__stop_all_projects() {
    echoinfo "Stopping all the projects"
    for _CS_PROJECT in $_CS_PROJECTS 
    do
        __stop_ee $_CS_PROJECT
    done

    __stop_activemq

    if (ps ax | grep java | grep -q "org.apache.cassandra") ;
    then
        echoinfo "Local Cassandra is running, shutting down..."
        echoinfo "Compacting Cassandra tables and flushing buffers, depending on the data volume this can take a long while (10GB node needs about 20 minutes)"
        $_CS_CASSANDRA_PRE_SHUTDOWN
        if ! (kill $(cat $_CS_CASSANDRA_PID));
        then
            echoerror "Unable to shutdown PID: "$(cat $_CS_CASSANDRA_PID)
        else
            echodebug "OK, SIGTERM sent"
        fi
        sleep $__DEFAULT_SLEEP
        sleep $__DEFAULT_SLEEP
        rm $_CS_CASSANDRA_PID
    fi

    if (ps ax | grep java | grep -q "org.elasticsearch") ;
    then
        echoinfo "Local ElasticSearch is running, shutting down..."
        if ! (kill $(cat $_CS_ELASTIC_PID));
        then
            echoerror "Unable to shutdown PID: "$(cat $_CS_ELASTIC_PID)
        else
            echodebug "OK, SIGTERM sent"
        fi
        sleep $__DEFAULT_SLEEP
        sleep $__DEFAULT_SLEEP
        rm $_CS_ELASTIC_PID
    fi
}

#---  FUNCTION  -------------------------------------------------------------------------------------------------------
#          NAME:  __pause_all_projects
#   DESCRIPTION:  Prepare all projects for backup: gracefully shutdown all local ExportExecutors and ActiveMQ,
#                 but leave Cassandra and ElasticSearch running
#----------------------------------------------------------------------------------------------------------------------
__pause_all_projects() {

    echoinfo "Stopping ExportExecutors for all projects"
    for _CS_PROJECT in $_CS_PROJECTS 
    do
        __stop_ee $_CS_PROJECT
    done

    echoinfo "Stopping local ActiveMQ"
    __stop_activemq

}


echodebug "Detected script full path: $__ScriptFullPath"
echodebug "Detected script full directory: $__ScriptDir"
echodebug "Detected Web Root: $_CS_WEBROOT"
echodebug "Detected Project list: $_CS_PROJECTS"

if ! [ "$USER" = "root" ] ;
then
    echo
    echoerror "Started from non-root account, please start this script as root or via sudo"
    exit 1
fi

case "$__ScriptArgs" in
    start)
        echoinfo "Starting..."
        __start_all_projects
        ;;
    stop|graceful-stop)
        echoinfo "Stopping..."
        __stop_all_projects
        ;;
    status)
        echoinfo "Status (per project)"
        ;;
    restart)
        echoinfo "Restarting..."
        __stop_all_projects
        __start_all_projects
        ;;
    pre-backup|pause)
        echoinfo "Pause for backup..."
        __pause_all_projects
        ;;
    post-backup|resume)
        echoinfo "Resuming after backup..."
        # we can use __start_all_projects function to resume, as it's idempotent 
        # and won't start already running Cassandra and ElasticSearch twice
        __start_all_projects
        ;;
    *)
        echo
        echowarn "Called without arguments"
        echo "\nUsage: $__ScriptFullName {start|stop|graceful-stop|restart|pre-backup|post-backup|status}\n" >&2
        exit 3
        ;;
esac
