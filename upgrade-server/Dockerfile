FROM openjdk:8-jdk

COPY target/upgrade-server-1.1.1.Final-SNAPSHOT-jar-with-dependencies.jar /upgrade-server-1.1.1.Final.jar
COPY src/main/resources/upgrade.properties /upgrade.properties

CMD ["java", "-Djava.net.preferIPv4Stack=true", "-Dcom.sun.management.jmxremote", "-jar","/upgrade-server-1.1.1.Final.jar", "-c", "/upgrade.properties"]

