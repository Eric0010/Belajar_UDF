Sesudah UDF dimasukan ke dalam ksqldb, saya akan menggunakannya di ksqldb dan sekaligus memasukan hasilnya di sebuah topic Kafka.

Pertama, saya akan membuat sebuah stream menggunakan topic yang berisikan hanya ip saja menggunakan command

```
CREATE STREAM ip_stream (
    ip STRING
) WITH (
    KAFKA_TOPIC = 'onlyip_test_v2',
    VALUE_FORMAT = 'JSON'
);
```

Kemudian, setelah stream ini sudah terdaftar di flow ksqldb. saya akan membuat 1 stream lagi yang akan menyimpan semua informasi tentang ip pada sebuah topik kafka dengan command

```
CREATE STREAM IP_GEOLOCATION_STREAM WITH (CLEANUP_POLICY='delete', KAFKA_TOPIC='IP_GEOLOCATION_STREAM', PARTITIONS=3, REPLICAS=1, RETENTION_MS=604800000, VALUE_FORMAT='JSON') AS SELECT
  IP_STREAM.IP QUERY_IP,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.status') STATUS,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.continent') CONTINENT,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.continentCode') CONTINENTCODE,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.country') COUNTRY,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.countryCode') COUNTRYCODE,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.region') REGION,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.regionName') REGIONNAME,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.city') CITY,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.district') DISTRICT,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.zip') ZIP,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.lat') LAT,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.lon') LON,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.timezone') TIMEZONE,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.offset') OFFSET,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.currency') CURRENCY,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.isp') ISP,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.org') ORG,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP_STREAM.IP), '$.as') AS_INFO
FROM IP_STREAM IP_STREAM
EMIT CHANGES;
```

Lalu, setelah membuat stream diatas. Saya akan melakukan sebuah query untuk memasukan data informasi ip ke dalam stream yang baru saja dibuat dengan command

```
INSERT INTO IP_GEOLOCATION_STREAM
SELECT 
  IP AS query_ip,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.status') AS status,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.continent') AS continent,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.continentCode') AS continentCode,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.country') AS country,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.countryCode') AS countryCode,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.region') AS region,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.regionName') AS regionName,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.city') AS city,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.district') AS district,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.zip') AS zip,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.lat') AS lat,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.lon') AS lon,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.timezone') AS timezone,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.offset') AS offset,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.currency') AS currency,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.isp') AS isp,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.org') AS org,
  EXTRACTJSONFIELD(GET_GEOLOCATION(IP), '$.as') AS as_info
FROM IP_STREAM;
```

Berikut adalah contoh flow yang ada di ksqldb:

![image](https://github.com/user-attachments/assets/18aedb16-e395-4f79-9027-92759c87002e)


Setelah semua command ini sudah berjalan, saya akan melakukan pengecekan terhadap topik kafka yang bernama IP_GEOLOCATION_STREAM, untuk melihat detail message yang dikirim. Topik default_ksql_processing_log, untuk melihat log ksql jika terjadi error. Sekaligus juga memonitoring CPU, RAM, Disk Usage dan Consumer Lag.

Memeriksa Consumer Lag juga penting untuk memastikan apakah pesan yang dikirim sudah sukses masuk kedalam topik atau mengalami failed/gagal.

Contoh Consumer Lag:

![WhatsApp Image 2025-03-03 at 09 29 00_1c741803](https://github.com/user-attachments/assets/3be1bbf0-20d2-4a29-aed3-eeeb767a2751)

Jumlah pesan yang ada di consumer lag bergantung kepada seberapa banyak ip yang berada pada stream yang hanya berisikan ip saja dan seberapa banyak stream yang kedua melakukan consume informasi tentang ip.

Gambar diatas menunjukan Consumer Lag ketika 1 juta ip sudah masuk ke stream yang pertama.

Berikut adalah salah satu contoh dari message yang sudah terkirim:

![WhatsApp Image 2025-03-03 at 09 28 59_316c1e1c](https://github.com/user-attachments/assets/988afcf0-2d57-42e9-9ae5-a3c338ffb0ab)





