package se.deadlock.txman;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

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

  public <T> Optional<T> executeOne(final String sql, final RowMapper<T> rowMapper) {
    return executeOne(sql, rowMapper, Collections.emptyList());
  }

  public <T> Optional<T> executeOne(final String sql, final RowMapper<T> rowMapper, final List<Object> parameters) {
    final List<T> results = execute(sql, rowMapper, parameters);
    if (results.size() > 1) {
      throw new RuntimeException("Multiple results returned! " + sql);
    }

    return results.size() == 1 ? Optional.of(results.get(0)) : Optional.empty();
  }

  public <T> List<T> execute(final String sql, final RowMapper<T> rowMapper) {
    return execute(sql, rowMapper, Collections.emptyList());
  }

  public <T> List<T> execute(final String sql, final RowMapper<T> rowMapper, final List<Object> parameters) {
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

  public int update(final String sql) {
    return update(sql, Collections.emptyList());
  }

  public int update(final String sql, final List<Object> parameters) {
    try {
      return prepareStatement(sql, parameters).executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private PreparedStatement prepareStatement(final String sql, final List<Object> parameters) throws SQLException {
    final String expandedSql = expandLists(sql, parameters);

    final PreparedStatement statement = connection.prepareStatement(expandedSql);
    for (int index = 0; index < parameters.size(); index++) {
      try {
        statement.setObject(index + 1, parameters.get(index));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return statement;
  }

  public static String expandLists(String sql, List<Object> parameters) {
    int lastParamStringIndex = -1;
    for (int index = 0; index < parameters.size(); index++) {
      Object param = parameters.get(index);
      lastParamStringIndex = sql.indexOf("?", lastParamStringIndex + 1);
      if (param instanceof List) {
        final StringJoiner joiner = new StringJoiner(", ");
        parameters.remove(index);
        for (final Object paramValue : (List)param) {
          joiner.add("?");
          parameters.add(index, paramValue);
          index++;
        }
        index--;
        sql = sql.substring(0, lastParamStringIndex) + joiner.toString() + sql.substring(lastParamStringIndex + 1);
        lastParamStringIndex += joiner.toString().length();
      }
    }
    return sql;
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
