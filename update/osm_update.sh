#!/bin/bash

set -e
set -x

description="OpenStreetMap database update script"
version="0.1/20171009"

# OSMOSIS
#
# Osmosis is the tool used to get change files from OSM repository
# Osmosis working directory contains 3 files:
#   * download.lock
#   * configuration.txt: osmosis configuration. maxInterval setting is used to
#     know how far osmosis is going into a single run. Deactivate that feature
#     by setting 0 (required for initial update run when planet file is old).
#   * state.txt: used to know which change files to download. That file is
#     updated at the end of processing for the next run.
#
# Warning! Osmosis is working into /tmp and downloaded files disk usage can be
# big if working on an old timestamp.
#
# Imposm
#
# Imposm is used to apply changes.osc compiled by Osmosis on the
# OpenStreetMap database.
#


# ----------------------------------------------------------------------------
# Settings

# Osmosis
FREQUENCY=day # from minute|hour|day
MAX_INTERVAL=0  # 3600 for an hour
STATE_SRV_URL=https://replicate-sequences.osm.mazdermind.de/
PLANET_URL=http://planet.openstreetmap.org/replication
CHANGE_FILE=changes.osc.gz

# Internal
W_DIR=/data/osmosis
EXEC_TIME=$(date '+%Y%m%d-%H%M%S')
LOG_DIR=$W_DIR/log
LOG_FILE=$LOG_DIR/${EXEC_TIME}.$(basename $0 .sh).log
LOG_MAXDAYS=3  # Log files are kept $LOG_MAXDAYS days
LOCK_FILE=$W_DIR/$(basename $0 .sh).lock
OSMOSIS=/usr/bin/osmosis
STOP_FILE=${W_DIR}/stop

# imposm
IMPOSM_CONFIG_FILE=/data/imposm/diff/config.json

# ----------------------------------------------------------------------------

usage () {
	echo "This is `basename $0` v$version"
	echo
	echo "    $description"
	echo
	echo "    usage: $0 <path to planet file>"
	echo "        INIT mode: initialize Osmosis directory and apply OSM diffs on database"
	echo
	echo "    usage: $0"
	echo "        UPDATE mode:"
	echo "        apply diffs on OSM database based on a already initialized"
	echo "        Osmosis working directory"
	echo
	echo "    usage: $0 -h"
	echo "        display help and version"
	echo
	echo "    Create a file named $(basename $STOP_FILE) into $W_DIR directory"
	echo "        to put process on hold."
	echo
	echo "    Dependencies: osmosis, imposm3"
	echo
	exit 0
}

log () {
	echo "[`date +"%Y-%m-%d %H:%M:%S"`] $$ :INFO: $1" >> $LOG_FILE
}

log_error () {
	echo "[`date +"%Y-%m-%d %H:%M:%S"`] $$ :ERROR: $1" >> $LOG_FILE

	rm $LOCK_FILE
	echo "[`date +"%Y-%m-%d %H:%M:%S"`] $$ :ERROR: restore initial state file" >> $LOG_FILE
	mv ${W_DIR}/.state.txt ${W_DIR}/state.txt &>/dev/null
	echo "[`date +"%Y-%m-%d %H:%M:%S"`] $$ :ERROR: $(basename $0) terminated in error!" >> $LOG_FILE

	# Message in stdout for console and cron
	echo "$(basename $0) (PID=$$) terminated in error!"
	echo "$1"
	echo "see $LOG_FILE for more details"

	exit 1
}

get_lock () {
	if [ -s $LOCK_FILE ]; then
		if ps -p `cat $LOCK_FILE` > /dev/null ; then
			return 1
		fi
	fi
	echo $$ > $LOCK_FILE
	return 0
}

free_lock () {
	rm $LOCK_FILE
}

# ----------------------------------------------------------------------------


START=$(date +%s)

TMP_DIR=${W_DIR}/.$(basename $0).${EXEC_TIME}
trap 'rm -rf ${TMP_DIR} &>/dev/null' EXIT
mkdir -p ${TMP_DIR}
mkdir -p ${LOG_DIR}
touch $LOG_FILE $LOCK_FILE

# Remove old log files
find ${LOG_DIR} -name "*.log" -mtime +$LOG_MAXDAYS -delete


# Help and configuration checks
[ "$1" == "-h" ] && usage
[ ! -f "$OSMOSIS" ] && log_error "$OSMOSIS not found"


log "new $(basename $0) process started"
log "working into directory: ${W_DIR}"


if [ -e $STOP_FILE ]; then
	log "$(basename $0) process held!"
	exit 1
fi

if ! get_lock ; then
	log "$(basename $0) process still running: PID=$(cat ${LOCK_FILE})"
	exit 1
fi

if [ ! -f $PGPASS ]; then
	log "ERROR: PostgreSQL user $PGUSER password file $PGPASS not found!"
	exit 1
fi

# Set running mode init or update
if [ ! -z $1 ]; then
	INIT_MODE=true
	log "running in INIT mode"
else
	INIT_MODE=false
	log "running in UPDATE mode"
fi


if [ $INIT_MODE = true ]; then

	# Check planet file exists
	if [ ! -f $1 ]; then
		log_error "file $1 not found"
	fi

	log "initializing osmosis working directory: ${W_DIR}"
	$OSMOSIS --read-replication-interval-init workingDirectory=${W_DIR} &>> $LOG_FILE

	log "extract timestamp from planet file: $1"
	TIMESTAMP=$(osmconvert $1 --out-timestamp)
	log "planet file timestamp is: ${TIMESTAMP}"
	# Rewind 2 hours
	TIMESTAMP_START=$(date -u -d @$(($(date -d "${TIMESTAMP}" '+%s') - 7200)) '+%FT%TZ')

	log "generate initial state file with date: ${TIMESTAMP_START}"
	# state.txt is the default name for osmosis
	wget -q --no-check-certificate \
		"${STATE_SRV_URL}?${TIMESTAMP_START}&stream=${FREQUENCY}" \
		-O ${W_DIR}/state.txt

	log "update configuration file"
        # configuration.txt is the default for osmosis
	echo "baseUrl=${PLANET_URL}/${FREQUENCY}" > ${W_DIR}/configuration.txt
	echo "maxInterval = ${MAX_INTERVAL}" >> ${W_DIR}/configuration.txt

else # Update mode: check if working directory is initialized correctly
	if [ ! -f ${W_DIR}/configuration.txt -o ! -f ${W_DIR}/state.txt ]; then
		log_error "osmosis working directory ${W_DIR} note initialized: please run into INIT mode before"
	fi
fi


log "generate changes file into ${TMP_DIR}/${CHANGE_FILE}"
log "backup of state file"
cp ${W_DIR}/state.txt ${W_DIR}/.state.txt

if ! $OSMOSIS --read-replication-interval workingDirectory=${W_DIR} \
	--simplify-change --write-xml-change \
	${TMP_DIR}/${CHANGE_FILE} &>> $LOG_FILE ; then

	log_error "osmosis failed"
fi

log "apply changes on OSM database"
log "${CHANGE_FILE} file size is $(ls -sh ${TMP_DIR}/${CHANGE_FILE} | cut -d' ' -f 1)"

if ! imposm3 diff -config $IMPOSM_CONFIG_FILE ${TMP_DIR}/${CHANGE_FILE} >> $LOG_FILE ; then

    log_error "imposm3 failed"
fi

free_lock

log "${CHANGE_FILE} file size is $(ls -sh ${TMP_DIR}/${CHANGE_FILE} | cut -d' ' -f 1)"
END=$(date +%s)
DURATION=$(($END-$START))
DURATION_STR=$(printf '%dh%02dm%02ds' $(($DURATION/3600)) $(($DURATION%3600/60)) $(($DURATION%60)))
log "$(basename $0) duration: $DURATION_STR"

log "$(basename $0) successfully terminated!"

exit 0
