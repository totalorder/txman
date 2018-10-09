package se.deadlock.txman;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;
import javax.sql.DataSource;

public class TxMan {
  private final DataSource dataSource;

  public TxMan(final DataSource dataSource) {
    this.dataSource = dataSource;
  }

  private Tx begin(final boolean readOnly) {
    try {
      final Connection connection = dataSource.getConnection();
      return new Tx(connection, readOnly);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T begin(final Function<Tx, T> callback, final boolean readOnly) {
    Tx tx = null;
    try {
      tx = begin(readOnly);
      return callback.apply(tx);
    } finally {
      if (tx != null) {
        tx.commitOrRollback();
      }
    }
  }

  public <T> T begin(final Function<Tx, T> callback) {
    return begin(callback, false);
  }

  public <T> T beginReadonly(final Function<Tx, T> callback) {
    return begin(callback, true);
  }
}
