#!/bin/bash
set -e

PROJECT_NAME=kafka
VERSION=1.0

usage(){
	echo "Environmental variable FLUME_HOME must be set to the Flume installation directory."
	exit 1
}

collectClasses(){
	find src -name \*.java -print > classes.list
}

compile(){
	collectClasses
	clean
	`mkdir -p $TARGET_CLASSES_DIR`
	echo "--- Generating class files. Destination directory $TARGET_CLASSES_DIR --"
	`javac -cp .:$CLASSPATH_DIR -d $TARGET_CLASSES_DIR @classes.list`
	`rm classes.list`
	echo "-- Classes generated successfully ---"
}

clean(){
	if [ -d "$TARGET_DIR" ]; then
		echo "--- Cleaning up directory $TARGET_DIR ---"
		`rm -rf $TARGET_DIR`
	fi
}

package(){
	echo "--- Building Packaging ---"
	mkdir -p $DESTINATION_DIR
	jar -cvf $DESTINATION_DIR/$JAR_NAME -C $TARGET_CLASSES_DIR .
	echo "--- $JAR_NAME built successfully ---"
	copyDependencies
	echo "---------------------------"
	echo "      Build Successful     "
	echo "---------------------------"
}

copyDependencies(){
	echo "--- Copying dependencies ---"
	#cp $CLASSPATH_DIR/*.jar $DESTINATION_DIR
}

if [ -z "$KAFKA_HOME" ]; then
	usage
fi

CLASSPATH_DIR=$KAFKA_HOME/lib/*
CLASSPATH_DIR=../../../ggjava/ggjava.jar:$CLASSPATH_DIR
TARGET_DIR=target
TARGET_CLASSES_DIR=$TARGET_DIR/classes
JAR_NAME=ogg-$PROJECT_NAME-adapter-$VERSION.jar
DESTINATION_DIR=$TARGET_DIR/$PROJECT_NAME-lib
compile
package
