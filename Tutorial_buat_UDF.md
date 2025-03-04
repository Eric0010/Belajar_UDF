Pertama, buatlah 1 folder yang baru dengan command ```mkdir```. Disini saya akan menamakan folder tersebut menjadi udfgeo.

![image](https://github.com/user-attachments/assets/85f024a8-2f9a-48b0-aa61-79dc29467131)

Kemudian setelah itu buatlah file pom.xml dan file java untuk function yang diinginkan. Disini saya akan memberi nama GeoLookupBatchUDF.java

NOTE: file .java harus diletakan sesuai dengan nama package yang ada di dalam file java.

Misalnya, saya mempunyai nama package seperti berikut:

![image](https://github.com/user-attachments/assets/e19daa95-a874-4229-b338-2352b91b5be8)

Maka saya akan membuat folder di linux, seperti berikut:

![image](https://github.com/user-attachments/assets/58bee0e2-603d-47dc-a324-03e018c4e8c5)

Jika hal ini tidak dilakukan dengan benar, maka ksqldb tidak akan membaca function yang akan kita masukan.

Disini, saya akan membuat UDF supaya mendapatkan informasi terkait dengan longtitude, latitude, kota, kabupaten dan lain - lain dari sebuah ip.

Berikut adalah file pom.xml dan file GeoLookupBatchUDF.java

pom.xml

```
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.ksqldb.udf</groupId>
    <artifactId>geo-lookup-udf</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <confluent.version>7.7.0</confluent.version>
    </properties>

    <dependencies>
        <!-- ksqlDB UDF API -->
        <dependency>
            <groupId>io.confluent.ksql</groupId>
            <artifactId>ksqldb-udf</artifactId>
            <version>${confluent.version}</version>
            <scope>provided</scope>
            <exclusions>
                <!-- Exclude telemetry-client to avoid missing dependency error -->
                <exclusion>
                    <groupId>io.confluent.observability</groupId>
                    <artifactId>telemetry-client</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Gson for JSON parsing -->
        <dependency>
            <groupId>com.google.code.gson</groupId>
            <artifactId>gson</artifactId>
            <version>2.10.1</version>
        </dependency>

        <!-- Apache HTTP Client -->
        <dependency>
            <groupId>org.apache.httpcomponents.client5</groupId>
            <artifactId>httpclient5</artifactId>
            <version>5.2</version>
        </dependency>

        <!-- Logging dependencies -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-api</artifactId>
            <version>2.17.2</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>2.17.2</version>
        </dependency>
    </dependencies>

    <repositories>
        <!-- Confluent Repository -->
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

    <build>
        <plugins>
            <!-- Compiler Plugin -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.8.1</version>
                <configuration>
                    <source>17</source>
                    <target>17</target>
                    <compilerArgs>
                        <arg>-parameters</arg>
                    </compilerArgs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

GeoLookupBatchUDF.java

```
package com.ksqldb.udf;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfDescription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

@UdfDescription(name = "get_geolocation", description = "Fetch geolocation data for IP addresses")
public class GeoLookupBatchUDF {

    private static final String API_BATCH_URL = "http://ip-api.com/batch?fields=status,message,continent,continentCode,country,countryCode,region,regionName,city,district,zip,lat,lon,timezone,offset,currency,isp,org,as,query";
    private static final String API_SINGLE_URL = "http://ip-api.com/json/";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Udf(description = "Fetch geolocation data for a list of IPs")
    public String getGeolocation(@UdfParameter(value = "ips", description = "List of IP addresses") final List<String> ips) {
        if (ips == null || ips.isEmpty()) {
            return "{\"error\": \"IP list cannot be empty\"}";
        }
        try {
            String requestBody = objectMapper.writeValueAsString(ips);
            URL url = new URL(API_BATCH_URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream();
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))) {
                writer.write(requestBody);
                writer.flush();
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                try (InputStream is = connection.getInputStream();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    JsonNode jsonNode = objectMapper.readTree(reader);

                    // Check for API failure response inside JSON
                    if (jsonNode.isArray() && jsonNode.get(0).has("status") &&
                            "fail".equals(jsonNode.get(0).get("status").asText())) {
                        return "{\"error\": \"" + jsonNode.get(0).get("message").asText() + "\"}";
                    }

                    return jsonNode.toString();
                }
            } else {
                return String.format("{\"error\": \"API request failed with status %d\"}", responseCode);
            }
        } catch (IOException e) {
            return String.format("{\"error\": \"Exception: %s\"}", e.getMessage());
        }
    }

    @Udf(description = "Fetch geolocation data for a single IP")
    public String getGeolocation(@UdfParameter(value = "ip", description = "IP address") String ip) {
        if (ip == null || ip.isEmpty()) {
            return "{\"error\": \"Invalid IP address\"}";
        }
        try {
            String singleIpUrl = API_SINGLE_URL + ip + "?fields=status,message,continent,continentCode,country,countryCode,region,regionName,city,district,zip,lat,lon,timezone,offset,currency,isp,org,as,query";
            URL url = new URL(singleIpUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");

            int responseCode = connection.getResponseCode();
            if (responseCode == 200) {
                try (InputStream is = connection.getInputStream();
                     BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    JsonNode jsonNode = objectMapper.readTree(reader);

                    // Check for API failure response inside JSON
                    if (jsonNode.has("status") && "fail".equals(jsonNode.get("status").asText())) {
                        return "{\"error\": \"" + jsonNode.get("message").asText() + "\"}";
                    }

                    return jsonNode.toString();
                }
            } else {
                return String.format("{\"error\": \"API request failed with status %d\"}", responseCode);
            }
        } catch (IOException e) {
            return String.format("{\"error\": \"Exception: %s\"}", e.getMessage());
        }
    }
}
```

NOTE: 

1. Pastikan juga versi Java antar file memiliki versi yang sama.

2. Nama Package di file .java dan nama GroupId di pom.xml harus memiliki nama yang sama.

Setlah file tersebut masing - masing disimpan di dalam folder yang sudah benar. Saya akan menjalankan command ```mvn clean package```. supaya 2 file tersebut menjadi jar. 

Lalu, cek status jar apakah sudah sukses dibuat. Jika sudah cek folder udfgeo kembali dan cek folder target maka file .jar akan terlihat di dalam folder tersebut.

![image](https://github.com/user-attachments/assets/6e1c76b3-4bd2-4639-ae99-1fefb0330d39)

Setelah itu, saya akan meng-copy file jar ke destinasi ini ```/home/kafka/confluent-7.7.0/share/java/ksqldb/ext``` dengan command:

```
cp target/ksqldb-udf-geolocation-1.0-SNAPSHOT.jar /home/kafka/confluent-7.7.0/share/java/ksqldb/ext
```

Jangan lupa untuk menambahkan konfigurasi di file properties ksql dimana destinasi yang tepat untuk meletakan .jar extension udf ini seperti gambar berikut:

![image](https://github.com/user-attachments/assets/e3c7447d-1017-4006-bdc1-27c9bd00d199)

Setelah semua hal tersebut dilakukan, saya akan merestart service ksqldb dengan command

```
sudo systemctl restart confluent-ksqldb
```

Setelah di restart, saya akan melakukan pengecekan apakah UDF sudah terbaca di ksqldb atau belum.

Pertama, masuk ke ksqldb dengan command

```
ksql http://server-eric:8088
```

lalu jalankan command:

```
SHOW FUNCTIONS;
```

Jika sudah tertambahkan, maka akan terlihat seperti gambar berikut:

![image](https://github.com/user-attachments/assets/3b07c635-24c2-42a6-b2d0-6610446c1879)

Jika fungsi sudah terlihat coba lakukan query untuk test function

```
SELECT GET_GEOLOCATION('8.8.8.8') FROM ip_stream EMIT CHANGES;
```

Hasil:

![image](https://github.com/user-attachments/assets/d8e408d2-6ebb-4f27-86e9-84cf616cc303)

NOTE: Perlu diketahui, ip_stream perlu dibuat terlebih dahulu dengan message yang berisikan hanya ip saja.




