package se.deadlock.txman;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import se.deadlock.composed.Composed;

class SimpleJdbcTest {
  @RegisterExtension
  static Composed postgres = TestUtil.postgres;

  static DataSource dataSource;
  static TxMan txMan;

  @BeforeAll
  static void beforeAll() {
    dataSource = TestUtil.getDataSource(postgres.externalPort(5432));

    txMan = new TxMan(dataSource);
    txMan.begin(tx -> {
      tx.update("DROP TABLE IF EXISTS jdbctest");
      tx.update("CREATE TABLE jdbctest (id SERIAL PRIMARY KEY, data TEXT NOT NULL)");
      return null;
    });
  }

  @Test
  void basicJdbc() {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
      final Statement statement = connection.createStatement();
      assertThat(statement.executeUpdate("INSERT INTO jdbctest (data) VALUES ('asd')"), is(1));

      final ResultSet rs = statement.executeQuery("SELECT * FROM jdbctest");

      final List<String> datas = new ArrayList<>();
      while (rs.next()) {
        final int id = rs.getInt("id");
        assertThat(id, greaterThanOrEqualTo(1));

        final String data = rs.getString("data");
        datas.add(data);
      }

      final List<String> expectedDatas = new ArrayList<>();
      expectedDatas.add("asd");
      assertThat(datas, is(expectedDatas));

    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
