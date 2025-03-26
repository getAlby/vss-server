FROM gradle:jdk17-jammy as builder

WORKDIR /build
COPY . ./
RUN gradle wrapper --gradle-version 8.1.1
RUN ./gradlew build -x test

##########################################################################
# Use official Tomcat base image
FROM tomcat:jre17

# Copy WAR file
COPY --from=builder /build/app/build/libs/vss-1.0.war /usr/local/tomcat/webapps/vss.war
# Add datadog tracing library for APM
ADD 'https://dtdg.co/latest-java-tracer' dd-java-agent.jar

EXPOSE 8080
CMD ["catalina.sh", "run"]
