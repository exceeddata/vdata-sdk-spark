# 1. 文档目的

帮助使用自定义数据源，用 spark sql 来读取查询 vsw 文件之中的数据。





# 2. 使用

## 2.1 配置pom，引入自定义数据源及相关依赖

创建一个maven 工程，在pom.xml 之中添加以下依赖：

```XML
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <!-- Replace the group ID with your group ID -->
    <groupId>com.exceeddata.example</groupId>
    <!-- Replace the artifact ID with the name of your project -->
    <artifactId>exceeddata-commons</artifactId>
    <version>${exceeddata.version}</version>
    <packaging>jar</packaging>
    <!-- The name should likely match the artifact ID -->
    <name>ExceedData Commons</name>
    <url>http://www.exceeddata.com</url>

    <properties>
        <maven.compiler.version>3.8.1</maven.compiler.version>
        <maven.assembly.version>2.6</maven.assembly.version>
        <maven.surefire.version>2.19.1</maven.surefire.version>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <exceeddata.version>4.7.0</exceeddata.version>
        <spark.artifact>2.11</spark.artifact>
        <!--    <spark.version>2.4.0.cloudera2</spark.version>-->
        <spark.version>2.4.8</spark.version>
    </properties>
<!--    <repositories>-->
<!--        <repository>-->
<!--            <id>scala-tools.org</id>-->
<!--            <name>Scala-tools Maven2 Repository</name>-->
<!--            <url>http://scala-tools.org/repo-releases</url>-->
<!--        </repository>-->
<!--        <repository>-->
<!--            <id>cloudera</id>-->
<!--            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>-->
<!--        </repository>-->
<!--    </repositories>-->
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>${maven.compiler.version}</version>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                    <encoding>${project.build.sourceEncoding}</encoding>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <dependencies>
        <dependency>
            <groupId>com.exceeddata.sdk</groupId>
            <artifactId>vdata-sdk-spark_v2_4</artifactId>
            <version>2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4</version>
        </dependency>

        <dependency>
            <groupId>com.exceeddata.sdk</groupId>
            <artifactId>vdata-sdk-java</artifactId>
            <version>2.8.3</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${spark.artifact}</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <dependency>
            <groupId>io.airlift</groupId>
            <artifactId>aircompressor</artifactId>
            <version>0.24</version>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_${spark.artifact}</artifactId>
            <version>${spark.version}</version>
        </dependency>

    </dependencies>
</project>

```

## 2.2 创建SparkSession，配置format为我们自定义的数据源及相关参数。

```Java
import com.exceeddata.sdk.vdata.spark.v2.VDataSparkConfig;
import com.exceeddata.sdk.vdata.spark.v2.VDataSparkDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
    public static void main(String[] args) {
        String path="sample_files/vsw/data_diff_freqency.vsw";

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLTest！！！")
                .master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark
                .read()
                .format(VDataSparkDataSource.class.getName())
                .option(VDataSparkConfig.INPUT_PATH,path)
                .option(VDataSparkConfig.INPUT_COLUMNS,"_device,_time,Signal_100Hz,Signal_20Hz,Signal_10Hz")
                .option(VDataSparkConfig.SCHEMA_NUM_MAXREAD,1)
                .option(VDataSparkConfig.DENSIFY_ROWS_AHEAD, 0)
                .load();
        df.show();
        spark.close();
    }
}

```

format(VDataSparkDataSource.class.getName())：指定spark sql 数据源为 vsw。

option添加参数：

VDataSparkConfig.INPUT_PATH：vsw输入路径，必填。

VDataSparkConfig.INPUT_COLUMNS：vsw 信号列。

_device,_time这两个列名为隐藏列，分别表示vin 号和时间。

VDataSparkConfig.SCHEMA_NUM_MAXREAD：未提供列表列时要读取的文件数，以确定列架构。默认值为 1。

VDataSparkConfig.DENSIFY_ROWS_AHEAD：填充参数，默认为0，不填充。

## Output 
Run the example!  You will see all datas in the vsw file if it works.


+-------+--------------------+------------+-----------+-----------+
|_device|               _time|Signal_100Hz|Signal_20Hz|Signal_10Hz|
+-------+--------------------+------------+-----------+-----------+
|   data|1970-01-01 08:00:...|         1.0|       null|       null|
|   data|1970-01-01 08:00:...|        null|      201.0|       null|
|   data|1970-01-01 08:00:...|         2.0|       null|       null|
|   data|1970-01-01 08:00:...|         3.0|       null|       null|
|   data|1970-01-01 08:00:...|         4.0|       null|       null|
|   data|1970-01-01 08:00:...|        null|       null|     1001.0|
|   data|1970-01-01 08:00:...|         5.0|       null|       null|
|   data|1970-01-01 08:00:...|         6.0|       null|       null|
|   data|1970-01-01 08:00:...|        null|      202.0|       null|