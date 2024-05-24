
## Introduction
This repository contains samples for EXD vData SDK for Spark (exceeddata-sdk-vdata).  vData is an edge database running on vehicles' domain controllers.  It stores signal data in a high-compression file format with the extension of .vsw.  EXD vData SDK offers vsw decoding capabilities in standard programming languages such as C++, [Java](https://github.com/exceeddata/sdk-vdata-java), [Python](https://github.com/exceeddata/sdk-vdata-python), [Javascript](https://github.com/exceeddata/sdk-vdata-javascript), and etc.  

The following sections demonstrates how to install and use the SDK.

## Table of Contents
- [System Requirement](#system-requirement)
- [License](#license)
- [Installation](#installation)
- [VSW File Examples](#vsw-file-examples)
- [Getting Help](#getting-help)
- [Contributing to EXD](#contributing-to-exd)

## System Requirement
- Spark 2.x / 3.x
- JDK 8

## License
The codes in the repository are released with [MIT License](LICENSE).

## Installation
Download and install the jar via mvn install 

```sh
garyshi@GarydeMacBook-Pro Downloads % mvn install:install-file -Dfile=exceeddata-vdata-sdk-spark-2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4.jar -DgroupId=com.exceeddata.sdk -DartifactId=vdata-sdk-spark_v2_4 -Dversion=2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4 -Dpackaging=jar
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] --------------------------------[ pom ]---------------------------------
[INFO] 
[INFO] --- maven-install-plugin:2.4:install-file (default-cli) @ standalone-pom ---
[INFO] Installing /Users/garyshi/Downloads/exceeddata-vdata-sdk-spark-2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4.jar to /Users/garyshi/.m2/repository/com/exceeddata/sdk/vdata-sdk-spark_v2_4/2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4/vdata-sdk-spark_v2_4-2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4.jar
[INFO] Installing /var/folders/22/3y2gqffd703_jww35_t0x77m0000gn/T/mvninstall16257380440343494835.pom to /Users/garyshi/.m2/repository/com/exceeddata/sdk/vdata-sdk-spark_v2_4/2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4/vdata-sdk-spark_v2_4-2.8.3-hadoop-3.0.0-cdh6.3.4-spark-2.4.0-cdh6.3.4.pom
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  0.145 s
[INFO] Finished at: 2024-05-24T14:28:23+08:00
[INFO] ------------------------------------------------------------------------



garyshi@GarydeMacBook-Pro Downloads % mvn install:install-file -Dfile=exceeddata-vdata-sdk-java-2.8.3.jar  -DgroupId=com.exceeddata.sdk -DartifactId=vdata-sdk-java -Dversion=2.8.3 -Dpackaging=jar
[INFO] Scanning for projects...
[INFO] 
[INFO] ------------------< org.apache.maven:standalone-pom >-------------------
[INFO] Building Maven Stub Project (No POM) 1
[INFO] --------------------------------[ pom ]---------------------------------
[INFO] 
[INFO] --- maven-install-plugin:2.4:install-file (default-cli) @ standalone-pom ---
[INFO] Installing /Users/garyshi/Downloads/exceeddata-vdata-sdk-java-2.8.3.jar to /Users/garyshi/.m2/repository/com/exceeddata/sdk/vdata-sdk-java/2.8.3/vdata-sdk-java-2.8.3.jar
[INFO] Installing /var/folders/22/3y2gqffd703_jww35_t0x77m0000gn/T/mvninstall4040870039845949465.pom to /Users/garyshi/.m2/repository/com/exceeddata/sdk/vdata-sdk-java/2.8.3/vdata-sdk-java-2.8.3.pom
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  0.141 s
[INFO] Finished at: 2024-05-24T14:30:54+08:00
[INFO] ------------------------------------------------------------------------

```

## Step By Step Guide
[Step By Step Guide CN](guide_cn.md)

## VSW File Examples
### Different Frequency Data  
- [data_diff_freqency.vsw](https://github.com/exceeddata/sdk-vdata-python/tree/sample_files/vsw/data_diff_freqency.vsw): sample data file with 10Hz, 20Hz, 100Hz datas. This file can be used in vswdecode.py and other language Examples.


## Getting Help
For usage questions, the best place to go to is [Github issues](https://github.com/exceeddata/sdk-vdata-spark/issues). For customers of EXCEEDDATA commercial solutions, you can contact [support](mailto:support@smartsct.com) for questions or support.

## Contributing to EXD
All contributions, bug reports, bug fixes, documentation improvements, code enhancements, and new ideas are welcome.

<hr>

[Go to Top](#table-of-contents)
