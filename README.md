# TxMan
Zero-dependency transaction-safe JDBC-connector

[![Build Status](https://travis-ci.com/totalorder/txman.svg?branch=master)](https://travis-ci.com/totalorder/txman)
[![Maven Central](https://img.shields.io/maven-central/v/se.deadlock/txman.svg)](https://search.maven.org/artifact/se.deadlock/txman)

## Usage

Setup and create a table
 
```java
DataSource dataSource = createDataSource(); // Pick your JDBC-DataSource of choice
TxMan txMan = new TxMan(dataSource);
txMan.begin(tx -> tx.update("CREATE TABLE record (key INT PRIMARY KEY, value TEXT)"));
```

Insert a row and query it in the same transaction
```java
List<String> records = txMan.begin(tx -> {
  tx.update("INSERT INTO record (key, value) VALUES (?, ?)", 123, "abc");
  return tx.execute("SELECT * FROM record", (ResultSet rs) -> rs.getString("value"));
});
System.out.println(records) // Prints ["abc"]
```

Map rows to objects

```java
// Given: class Record { int id; String value; }

List<Record> records = txMan.begin(tx -> 
  tx.execute("SELECT * FROM record", rs -> new Record(rs.getInt("id"), rs.getString("value")))
);
System.out.println(records) // Prints [Record(id=123,value="abc")]
```

Get single row

```java
// Optionally store the mapping function for re-use
RowMapper<Record> rowMapper = rs -> new Record(rs.getInt("id"), rs.getString("value"));

Record record = txMan.begin(tx -> tx.executeOne("SELECT * FROM record LIMIT 1", rowMapper));
System.out.println(record) // Prints Record(id=123,value="abc")
```

Rollback a transaction
```java
txMan.begin(tx -> { 
  tx.update("INSERT INTO record (key, value) VALUES (?, ?)", 123, "abc");
  tx.setRollback();
  return null;
});
```

## Example Postgres data source
<caption>build.yml (gradle)</caption>

```groovy
dependencies {
    compile 'se.deadlock:txman:0.0.1'
    compile 'org.postgresql:postgresql:42.2.5'
    compile 'com.zaxxer:HikariCP:3.2.0'
}
```

<caption>HikariDataSourceTest.java</caption>

```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Optional;
import javax.sql.DataSource;
import se.deadlock.txman.TxMan;

public class HikariDataSourceTest {
  public static void main(String[] args) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql://localhost:5432/testdb");
    config.setUsername("testuser");
    config.setPassword("testpassword");
    DataSource dataSource = new HikariDataSource(config);

    TxMan txMan = new TxMan(dataSource);

    Optional<Integer> one = txMan.begin(tx -> tx.executeOne("SELECT 1", rs -> rs.getInt(1)));
    System.out.println("Postgres says: " + one.get());
  }
}
```

