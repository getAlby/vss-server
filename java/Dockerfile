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
COPY tomcat-config/server.xml /usr/local/tomcat/conf/server.xml

# All the below are defaults.
# Pass real values as env variables.
#ENV vss.jdbc.url="jdbc:postgresql://postgres:5432/postgres"
#ENV vss.jdbc.username=postgres
#ENV vss.jdbc.password=YOU_MUST_CHANGE_THIS_PASSWORD
#ENV vss.jwt.pubkey="-----BEGIN PUBLIC KEY-----MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAq3t7q3HXhyTWS0nWnY+YIYqEwh/Z/Jtwk0DgkqxF455gdzVlSyLyz5NQfXua1jW437/SMEcbHLWcwjxcowj1jvh9blGpvx+xPNH72J5ruDzrh5fhoq2XC7zNt1UVcjkMIlddP4pwK4fV5FrxOWvmxst3Ngp6ShNg5H0yiMTDBF+QqFhRlVqnO4IrIKczxd/VxCXSKJvKjM357n0PVD1KYFT3FJ5fN+d7Fdko16NbfQDDPsfQchfLAF2Tn/r4KZFzCovCQAt7cKDLHl87TvoHVZ4QBGHDIk/w1cig/gERtTqHECVg+wVctWfx6lb+9YG/4/9UgTQpDxAWVaFVd49CwQIDAQAB-----END PUBLIC KEY-----"

EXPOSE 8080
CMD ["catalina.sh", "run"]
