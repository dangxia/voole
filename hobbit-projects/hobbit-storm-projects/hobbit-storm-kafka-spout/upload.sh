#/bin/bash
s="m"
cd /media/hexh/75B401CD21D96BBD/develop/git/dev-git/hobbit-projects/hobbit-storm-projects/hobbit-storm-kafka-spout
mvn clean package
source util_scp.sh target/hobbit-storm-kafka-spout-0.0.1-SNAPSHOT.jar /tmp/hexh
