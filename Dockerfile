FROM docker.io/openjdk:8-alpine

MAINTAINER liyong yli@yihecloud.com

WORKDIR /program

COPY docker/startup.sh /program/startup.sh
RUN chmod +x /program/startup.sh

COPY target/openbridge-monitor.jar /program/app.jar

CMD /program/startup.sh