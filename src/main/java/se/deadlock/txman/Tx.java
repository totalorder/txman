package se.deadlock.txman;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Tx {
  private final Connection connection;
  public final boolean readOnly;
  private boolean rollback = false;

  Tx(final Connection connection, final boolean readOnly) {
    this.readOnly = readOnly;
    try {
      connection.setAutoCommit(readOnly);
      connection.setReadOnly(readOnly);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    this.connection = connection;
  }

  public <T> Optional<T> executeOne(final String sql, final RowMapper<T> rowMapper, final Object... parameters) {
    final List<T> results = execute(sql, rowMapper, parameters);
    if (results.size() > 1) {
      throw new RuntimeException("Multiple results returned! " + sql);
    }

    return results.size() == 1 ? Optional.of(results.get(0)) : Optional.empty();
  }

  public <T> List<T> execute(final String sql, final RowMapper<T> rowMapper, final Object... parameters) {
    try {
      final ResultSet resultSet = prepareStatement(sql, parameters).executeQuery();
      final List<T> results = new ArrayList<>();
      while (resultSet.next()) {
        results.add(rowMapper.mapRow(resultSet));
      }
      return results;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public int update(final String sql, final Object... parameters) {
    try {
      return prepareStatement(sql, parameters).executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private PreparedStatement prepareStatement(final String sql, final Object... parameters) throws SQLException {
    final PreparedStatement statement = connection.prepareStatement(sql);
    for (int index = 0; index < parameters.length; index++) {
      try {
        statement.setObject(index + 1, parameters[index]);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return statement;
  }

  public void setRollback() {
    rollback = true;
  }

  void commitOrRollback() {
    try {
      try {
        if (readOnly) {
          return;
        }

        if (rollback) {
          connection.rollback();
        } else {
          connection.commit();
        }
      } finally {
        if (!connection.isClosed()) {
          connection.close();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
