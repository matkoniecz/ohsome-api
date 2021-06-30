package org.heigit.ohsome.ohsomeapi.oshdb;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;

/** Holds the database connection objects. */
public class DbConnData {

  public static OSHDBDatabase db = null;
  public static OSHDBJdbc keytables = null;
  public static TagTranslator tagTranslator = null;
  public static RemoteTagTranslator mapTagTranslator = null;
  public static HikariConfig keytablesDbPoolConfig = null;
  public static HikariDataSource dataSource = null;

  public static Connection getKeytablesConn() throws SQLException {
    if (db instanceof OSHDBH2) {
      return ((OSHDBH2) db).getConnection();
    }
    return dataSource.getConnection();
  }

  private DbConnData() {
    throw new IllegalStateException("Utility class");
  }
}
