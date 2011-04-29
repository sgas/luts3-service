#!/bin/sh

# nagios script for monitoring registration of records into SGAS.
# options:
# -H host           # host to monitor for (machine name in the record). Mandatory
# -m monitorurl     # sgas monitor url to get information from. Mandatory
# -e hostcert       # file with host certificate
# -k hostkey        # file with host key
# -a cafile         # file with certificate authority
# -w warntime       # warning time period
# -c crittime       # critical time period

# Some defaults

HOSTCERT=/etc/grid-security/hostcert.pem
HOSTKEY=/etc/grid-security/hostkey.pem
CADIR=/etc/grid-security/certificates/

WARNTIME=172800 # 2 days
CRITTIME=345600 # 4 days

# read command line params
#while getopts "H:m:e:k:a:w:c" option
while getopts "H:m:e:k:a:w:c:" option
do
    case $option in
        H) HOST=$OPTARG ;;
        m) MONITOR_URL=$OPTARG ;;
        e) HOSTCERT=$OPTARG ;;
        k) HOSTKEY=$OPTARG ;;
        a) CADIR=$OPTARG ;;
        w) WARNTIME=$OPTARG ;;
        c) CRITTIME=$OPTARG ;;
    esac
done

if [ -z $HOST ]
then
    echo "Host parameter not specified";
    exit 3;
fi

if [ -z $MONITOR_URL ]
then
    echo "Monitor URL parameter not specified";
    exit 3;
fi

#echo  $HOST $MONITOR_URL $HOSTCERT $HOSTKEY $CADIR $WARNTIME $CRITTIME

PAYLOAD=`curl -s -f --capath $CADIR --cert $HOSTCERT --key $HOSTKEY $MONITOR_URL/$HOST` || { echo "Error retrieving monitoring data." ; exit 3; }

# read value, exit code 3 is unknown
SECONDS=`echo $PAYLOAD | python -c "import sys;import json; print json.load(sys.stdin)['registration_epoch']" 2>/dev/null` || { echo "Error parsing JSON payload" ; exit 3; }

if [ "$SECONDS" = "None" ]
then
    echo "No registrations for host: $HOST is not registering records";
    exit 1; # nagios warning
fi

if [ "$SECONDS" -gt "$CRITTIME" ]
then
    echo "Registration epoch exceeded critical period: $HOST is not registering records.";
    exit 2; # nagios critial
fi

if [ "$SECONDS" -gt "$WARNTIME" ]
then
    echo "Registration epoch exceeded warning period: $HOST is not registering records.";
    exit 1; # nagios warning
fi

# oll' correct
exit 0;

