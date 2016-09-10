FROM docker.yihecloud.com/base/tomcat:2.0_201607181506
MAINTAINER zhaoyq zhaoyongqiang@yihecloud.com

WORKDIR /program

COPY docker/startup.sh /program/startup.sh
RUN chmod +x /program/startup.sh

COPY target/bigdata-0.0.1-SNAPSHOT.jar /data/app.jar

CMD /program/startup.sh