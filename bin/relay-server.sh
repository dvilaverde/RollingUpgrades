#!/bin/bash

DIR=`dirname $0`
RELAY_DIR="$DIR/../relay-server"

cd $RELAY_DIR && mvn -o exec:java -Dexec.mainClass=org.jgroups.relay_server.RelayServer