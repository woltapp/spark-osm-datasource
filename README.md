# Spark OSM PBF datasource

[OSM PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format) spark datasource.

## Rationale

Opestreetmap is a literally planet scale database storing features found on a Earth surfaces. Commonly used OSM processing software
either uses single host processing or converts and stores OSM data to some database engine and uses it as a source of truth for distributed
processing. With Spark abilities to automatically distribute huge dataset over hundreds of nodes and process more data, that a
single node could fit, it sounds pretty logical to implement native Spark OSM PBF data source  
## Download

Supported scala versions are 2.11 and 2.12

### Maven 
        
         <dependency>
                <groupId>com.wolt.osm</groupId>
                <artifactId>spark-osm-datasource_2.11</artifactId>
                <version>0.3.0</version>
         </dependency>

         <dependency>
                <groupId>com.wolt.osm</groupId>
                <artifactId>spark-osm-datasource_2.12</artifactId>
                <version>0.3.0</version>
         </dependency>
        
### Gradle

         compile group: 'com.wolt.osm', name: 'spark-osm-datasource_2.11', version: '0.3.0'
         compile group: 'com.wolt.osm', name: 'spark-osm-datasource_2.12', version: '0.3.0'
        
### SBT 
                        
        libraryDependencies += "com.wolt.osm" % "spark-osm-datasource" % "0.3.0"
        
### Github release

        https://github.com/woltapp/spark-osm-datasource/releases/tag/v0.31.0
        
## Usage                
        
OSM PBF data source uses [parallel OSM PBF parser](https://github.com/woltapp/parallelpbf) internally, therefore it needs to provide
it with a input stream. OSM DataSource internally uses HDFS driver, so it will accept any HDFS compatible path, like local
file, http link, s3 link etc and convert it to the inputstream. The stream will be opened and read as many times, as many
partitions are requested:

    val spark = SparkSession
      .builder()
      .appName("OsmReader")
      .config("spark.master", "local[4]")
      .getOrCreate()

    val osm = spark.read.option("partitions", 256).format(OsmSource.OSM_SOURCE_NAME).load("s3://maps/extract-cesko-brno.osm.pbf").drop("INFO").persist(StorageLevel.MEMORY_AND_DISK)
    
There are two options for the reader:

* `partitions` - Number of partitions to split OSM PBF file. Pay attentions, that full planet file is about 1.2TB of uncompressed data, so plan
your partitioning accordingly.
* `threads` - Number of threads to use by [parallel OSM PBF parser](https://github.com/woltapp/parallelpbf). Keep it
`1` for a local deployments or set to number of cores per worker node.
* `useLocalFile` - Enables Spark file distribution mechanics, see below.

### Using with Spark file distirbution 

Another one option is to use Sparks ability to cache file on the nodes and read it locally. With that approach under some circumstances (repeated re-reading
of same set of big files in different steps) a better performance can be achieved.

        spark.sparkContext.addFile("s3://maps/extract-cesko-brno.osm.pbf")
        val osm = spark.read.option("partitions", 256).option("useLocalFile", "true").format(OsmSource.OSM_SOURCE_NAME).load("extract-cesko-brno.osm.pbf")
        
Please keep in mind, that you should specify just a file name as the `load` parameter, not a full path.
        
## Schema

Due to Spark processing paradigm, all types of the OSM entities are mapped to the same dataframe of the following structure:

    StructField("ID", LongType, nullable = false),
    StructField("TAG", MapType(StringType, StringType, valueContainsNull = false), nullable = false),
    StructField("INFO", StructType(    StructField("UID", IntegerType, nullable = true),
                                       StructField("USERNAME", StringType, nullable = true),
                                       StructField("VERSION", IntegerType, nullable = true),
                                       StructField("TIMESTAMP", LongType, nullable = true),
                                       StructField("CHANGESET", LongType, nullable = true),
                                       StructField("VISIBLE", BooleanType, nullable = false)
                                        ), nullable = true),
    StructField("TYPE", IntegerType, nullable = false),
    StructField("LAT", DoubleType, nullable = true),
    StructField("LON", DoubleType, nullable = true),
    StructField("WAY", ArrayType(LongType, containsNull = false), nullable = true),
    StructField("RELATION", ArrayType(StructType(    StructField("ID", LongType, nullable = false),
                                                     StructField("ROLE", StringType, nullable = true),
                                                     StructField("TYPE", IntegerType, nullable = false)
                                                 ), containsNull = false), nullable = true)

OSM nodes will have `LAT` and `LON` fields set and `WAY` and `RELATION` fields will be null.
OSM nodes will have `WAY` fields set and `LAT`/`LON` and `RELATION` fields will be null.
OSM relations will have `RELATION` fields set and `LAT`/`LON` and `WAYS` fields will be null.

`TYPE` field in both `RELATION` structure and in dataframe structure indicates type of the OSM entity. It is an
integer field with following values:

* 0 - Item is a OSM Node entity
* 1 - Item is a OSM Way entity
* 2 - Item is a OSM Relation entity

Reader supports column pruning during read, so dropped columnts will not be loaded from the OSM PBF file. Even more,
if both `LAT` and `LON` columns will be dropped, no node processing will be started at all. Same applies for
`WAY` and `RELATION` columnn.

### Warning on order instability

OSM PBF file can be sorted and stored in a ordered way. Unfortunately, due to parallel nature of the Spark, that 
ordering will be broken during parsing and several consequent dataframe loads may return data in a different order for 
each run. In case order is important for you, you can sort the dataframe after loading. 
                          |

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/woltapp/spark-osm-reader/tags). 

## Authors

* **Denis Chaplygin** - *Initial work* - [akashihi](https://github.com/akashihi)

## License

This project is licensed under the GPLv3 License - see the LICENSE file for details.