#Description
This is an example of k-means running with Mahout 0.9 and Hadoop 1.2.1. The example is executed in eclipse.

#Setup
  - open terminal
  - `mvn clean compile install`
  - set input and output path in **_main.java**
  - cmd + shift + F11

#Maven
To use maven, the appropriate setting has been added to **pom.xml**, with dependencies as following:
```
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-core</artifactId>
  <version>1.2.1</version>
</dependency>
<dependency>
  <groupId>org.apache.mahout</groupId>
  <artifactId>mahout-core</artifactId>
  <version>0.9</version>
</dependency>
```
