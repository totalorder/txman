package se.deadlock.txman;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import se.deadlock.composed.Composed;

class TxManTest {
  @RegisterExtension
  static Composed postgres = TestUtil.postgres;

  static DataSource dataSource;
  static TxMan txMan;

  @BeforeAll
  static void beforeAll() {
    dataSource = TestUtil.getDataSource(postgres.externalPort(5432));

    txMan = new TxMan(dataSource);
    txMan.begin(tx -> {
      tx.update("DROP TABLE IF EXISTS record");
      tx.update("CREATE TABLE record (key INT PRIMARY KEY, value TEXT NOT NULL)");
      return null;
    });
  }

  @AfterAll
  static void afterAll() {
    txMan.begin(tx -> tx.update("DROP TABLE IF EXISTS record"));
  }

  @BeforeEach
  void setUp() {
    txMan.begin(tx -> {
      tx.update("DELETE FROM record");
      return null;
    });
  }

  @Test
  void createDataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:postgresql://localhost:" + postgres.externalPort(5432) + "/txmantest");
    config.setUsername("txmantest");
    config.setPassword("txmantest");
    DataSource dataSource = new HikariDataSource(config);
    TxMan txMan = new TxMan(dataSource);
    Optional<Integer> one = txMan.begin(tx -> tx.executeOne("SELECT 1", rs -> rs.getInt(1)));

    assertThat(one, is(Optional.of(1)));
  }

  @Test
  void execute() {
    final List<Record> records = txMan.begin(tx -> {
      assertThat(tx.update("INSERT INTO record (key, value) VALUES (:key, :value)", tx.params()
          .put("key", 123)
          .put("value", "abc")
          .build()), is(1));
      return tx.execute("SELECT * FROM record", recordMapper);
    });

    assertThat(records, is(listOf(new Record(123, "abc"))));
  }

  @Test
  void executeInList() {
    final List<Record> records = txMan.begin(tx -> {
      assertThat(tx.update("INSERT INTO record (key, value) VALUES (:key, :value)", tx.params()
          .put("key", 123)
          .put("value", "abc")
          .build()), is(1));
      return tx.execute("SELECT * FROM record WHERE key IN (:keys) AND value = :value", tx.params()
              .put("keys", listOf(123, 456))
              .put("value", "abc")
              .build(),
          recordMapper);
    });

    assertThat(records, is(listOf(new Record(123, "abc"))));
  }

  @Test
  void executeReadOnly() {
    txMan.begin(tx -> tx.update("INSERT INTO record (key, value) VALUES (:key, :value);", tx.params()
        .put("key", 123)
        .put("value", "abc")
        .build()));

    final List<Record> records = txMan.beginReadonly(tx ->
        tx.execute("SELECT * FROM record", recordMapper));

    assertThat(records, is(listOf(new Record(123, "abc"))));
  }

  @Test
  void expandStatement() {
    final Map<String, Object> parameters = new HashMap<>();
    parameters.put("key", "abc");
    parameters.put("keys", listOf(123, 456));
    parameters.put("value", "def");
    parameters.put("values", listOf(789, 123));

    final Tx.ExpandedStatement expandedStatement = Tx.expandStatement(
        "SELECT * FROM record WHERE key = :key AND key IN (:keys) AND value = :value AND value IN (:values)",
        parameters);

    assertThat(expandedStatement.sql, is("SELECT * FROM record WHERE key = ? AND key IN (?, ?) AND value = ? AND value IN (?, ?)"));
    assertThat(expandedStatement.parameters, is(listOf("abc", 123, 456, "def", 789, 123)));
  }

  @Test
  void executeRollback() {
    final List<Record> records = txMan.begin(tx -> {
      assertThat(tx.update("INSERT INTO record (key, value) VALUES (:key, :value)", tx.params()
          .put("key", 123)
          .put("value", "abc")
          .build()), is(1));
      final List<Record> result = tx.execute("SELECT * FROM record", recordMapper);
      tx.setRollback();
      return result;
    });

    assertThat(records, is(listOf(new Record(123, "abc"))));

    final List<Record> recordsFromNewTx = txMan.beginReadonly(tx ->
        tx.execute("SELECT * FROM record", recordMapper));
    assertThat(recordsFromNewTx, is(listOf()));
  }

  class Record {
    private final int key;
    private final String value;

    Record(final int key, final String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      final Record record = (Record) o;
      return key == record.key &&
          Objects.equals(value, record.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }
  }

  private RowMapper<Record> recordMapper = (resultSet) -> new Record(
      resultSet.getInt("key"),
      resultSet.getString("value")
  );

  private <T> List<T> listOf(T... elements) {
    return new ArrayList<>(Arrays.asList(elements));
  }
}