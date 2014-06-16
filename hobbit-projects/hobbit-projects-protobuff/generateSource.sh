#!/bin/bash
SSH_DIR=`dirname $BASH_SOURCE`
target_path=$SSH_DIR/src/main/java/
source_path=$SSH_DIR/src/main/resources/proto/*.proto
protoc --java_out=src/main/java/ src/main/resources/proto/*.proto
#protoc --java_out=src/main/java/ src/main/resources/proto/simple/*.proto
