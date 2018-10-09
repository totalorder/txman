package se.deadlock.txman;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import se.deadlock.composed.Composed;

class TestUtil {
  static Composed postgres = Composed.builder()
      .projectName("txmantest")
      .dockerComposeFilePath("src/test/resources/docker-compose.yml")
      .serviceName("postgres")
      .healtCheck(TestUtil.postgresHealthcheck())
      .build();

  static DataSource getDataSource(final int port) {
    final HikariConfig dbConfig = new HikariConfig();
    dbConfig.setJdbcUrl("jdbc:postgresql://localhost:" + port + "/txmantest");
    dbConfig.setUsername("txmantest");
    dbConfig.setPassword("txmantest");
    dbConfig.setMaximumPoolSize(10);
    dbConfig.addDataSourceProperty("cachePrepStmts", "true");
    dbConfig.addDataSourceProperty("prepStmtCacheSize", "250");
    return new HikariDataSource(dbConfig);
  }

  static HealthCheck<Container> postgresHealthcheck() {
    return (container) -> {
      Connection connection = null;
      try {
        connection = DriverManager.getConnection(
            "jdbc:postgresql://localhost:" + postgres.externalPort(5432) + "/txmantest", "txmantest", "txmantest");
        final Statement statement = connection.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT 1");
        rs.next();
        assert rs.getInt(1) == 1;
        return SuccessOrFailure.success();
      } catch (final Exception e) {
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException ignored) { }
        }
        return SuccessOrFailure.fromException(e);
      }
    };
  }
}
