#!/bin/sh

#trap errors
set -e

HBASE_HOME=~/apps/hbase

PROFILE="cdh4.5.1"
DEBUG=""
XMX="3072m"
while (("$#")); do
				case "$1" in
					-c | clean )
									CLEAN="clean"
									;;
					-p )
									shift
									PROFILE="$1"
									;;
					--test )
									RUN_TESTS="run"
									;;
					-X )
									DEBUG="-X"
									;;
					* )
									echo "Unknown arg: $1"
									exit 65
	esac
	shift
done

MVN_COMMAND="install"

if [ $CLEAN ]; then
				MVN_COMMAND="clean $MVN_COMMAND"
fi

if [ -z $RUN_TESTS ]; then
				MVN_COMMAND="$MVN_COMMAND -DskipTests=true"
fi

MVN_COMMAND="$MVN_COMMAND $DEBUG"

BUILD_DIR=$(pwd)

MAVEN_OPTS="-Xmx$XMX"
mvn $MVN_COMMAND
find . -name '*.jar' | xargs -I {} cp {} ~/apps/hbase/lib


cd $HBASE_HOME
if [ $CLEAN ]; then
	./restartHbase.sh clean
else
				./restartHbase.sh
fi
cd $BUILD_DIR
#
