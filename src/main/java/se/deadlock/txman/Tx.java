package se.deadlock.txman;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    return executeOne(sql, Collections.emptyMap(), rowMapper);
  }

  public <T> Optional<T> executeOne(final String sql, final Map<String, Object> parameters, final RowMapper<T> rowMapper) {
    final List<T> results = execute(sql, parameters, rowMapper);
    if (results.size() > 1) {
      throw new RuntimeException("Multiple results returned! " + sql);
    }

    return results.size() == 1 ? Optional.of(results.get(0)) : Optional.empty();
  }

  public <T> List<T> execute(final String sql, final RowMapper<T> rowMapper) {
    return execute(sql, Collections.emptyMap(), rowMapper);
  }

  public <T> List<T> execute(final String sql, final Map<String, Object> parameters, final RowMapper<T> rowMapper) {
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
    return update(sql, Collections.emptyMap());
  }

  public int update(final String sql, final Map<String, Object> parameters) {
    try {
      return prepareStatement(sql, parameters).executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private PreparedStatement prepareStatement(final String sql, final Map<String, Object> parameters) throws SQLException {
    final ExpandedStatement expandedStatement = expandStatement(sql, parameters);

    final PreparedStatement statement = connection.prepareStatement(expandedStatement.sql);
    for (int index = 0; index < expandedStatement.parameters.size(); index++) {
      try {
        statement.setObject(index + 1, expandedStatement.parameters.get(index));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
    return statement;
  }

  static class ParameterPlacement {
    public final int index;
    public final String name;
    public final Object value;

    ParameterPlacement(final int index, final String name, final Object value) {
      this.index = index;
      this.name = name;
      this.value = value;
    }
  }

  static class ExpandedStatement {
    public final String sql;
    public final List<Object> parameters;

    ExpandedStatement(final String sql, final List<Object> parameters) {
      this.sql = sql;
      this.parameters = parameters;
    }
  }

  public static ExpandedStatement expandStatement(String sql, Map<String, Object> parameters) {
    final List<ParameterPlacement> placements = new ArrayList<>();
    for (final Map.Entry<String, Object> parameter : parameters.entrySet()) {
      final Pattern pattern = Pattern.compile(":" + parameter.getKey() + "($|[^a-zA-Z-_])");
      final Matcher matcher = pattern.matcher(sql);

      while (matcher.find()) {
        placements.add(new ParameterPlacement(matcher.start(), parameter.getKey(), parameter.getValue()));
      }
    }

    placements.sort(Comparator.comparing(placement -> placement.index));

    final List<Object> parametersList = new ArrayList<>();

    int additionLength = 0;
    for (final ParameterPlacement placement : placements) {
      final String addition;
      if (placement.value instanceof List) {
        final StringJoiner joiner = new StringJoiner(", ");
        for (final Object item : (List)placement.value) {
          parametersList.add(item);
          joiner.add("?");
        }
        addition = joiner.toString();
      } else {
        parametersList.add(placement.value);
        addition = "?";
      }
      sql = sql.substring(0, placement.index + additionLength) +
          addition +
          sql.substring(placement.index + additionLength + placement.name.length() + 1);
      additionLength += addition.length() - (placement.name.length() + 1);
    }

    return new ExpandedStatement(sql, parametersList);
  }

  public MapBuilder params() {
    return new MapBuilder();
  }

  public static class MapBuilder {
    private Map<String, Object> map = new HashMap<>();

    public MapBuilder put(final String key, final Object value) {
      map.put(key, value);
      return this;
    }

    public Map<String, Object> build() {
      return map;
    }
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
