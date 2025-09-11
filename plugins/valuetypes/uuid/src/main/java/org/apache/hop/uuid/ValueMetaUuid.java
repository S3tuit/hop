package org.apache.hop.uuid;

import java.io.*;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.UUID;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopEofException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaPlugin;

@ValueMetaPlugin(
    id = "77",
    name = "UUID",
    description = "Universally Unique Identifier",
    image = "images/inet.svg")
public class ValueMetaUuid extends ValueMetaBase {

  public static final int TYPE_UUID = 77;

  public ValueMetaUuid() {
    super(null, TYPE_UUID);
  }

  public ValueMetaUuid(String name) {
    super(name, TYPE_UUID);
  }

  public ValueMetaUuid(ValueMetaUuid meta) {
    super(meta.name, TYPE_UUID);
  }

  @Override
  public ValueMetaUuid clone() {
    return new ValueMetaUuid(this);
  }

  @Override
  public Class<?> getNativeDataTypeClass() {
    return UUID.class;
  }

  @Override
  public Object convertData(IValueMeta meta2, Object data2) throws HopValueException {
    if (data2 == null) {
      return null;
    } else if (data2 instanceof UUID) {
      return data2;
    }

    // fall back to String parse
    try {
      return UUID.fromString(data2.toString().trim());
    } catch (IllegalArgumentException ex) {
      throw new HopValueException(
          this + " : I can't convert the specified value to data type : " + "UUID");
    }
  }

  @Override
  public int hashCode(Object object) throws HopValueException {
    final UUID u = (UUID) convertData(this, object);
    return u == null ? 0 : u.hashCode();
  }

  @Override
  public Object cloneValueData(Object object) throws HopValueException {
    // UUIDs are immutable, cloning is unnecessary
    return object;
  }

  @Override
  public int compare(Object data1, Object data2) throws HopValueException {
    boolean n1 = isNull(data1);
    boolean n2 = isNull(data2);

    if (n1 && !n2) {
      if (isSortedDescending()) {
        return 1;
      } else {
        return -1;
      }
    }
    if (!n1 && n2) {
      if (isSortedDescending()) {
        return -1;
      } else {
        return 1;
      }
    }
    if (n1 && n2) {
      return 0;
    }

    int cmp = 0;

    cmp = typeCompare(data1, data2);

    if (isSortedDescending()) {
      return -cmp;
    } else {
      return cmp;
    }
  }

  protected int typeCompare(Object object1, Object object2) throws HopValueException {
    final UUID u1 = (UUID) convertData(this, object1);
    final UUID u2 = (UUID) convertData(this, object2);
    // UUID implements Comparable
    return u1.compareTo(u2);
  }

  @Override
  public String getString(Object object) throws HopValueException {
    UUID u = (UUID) convertData(this, object);
    return u == null ? null : u.toString();
  }

  @Override
  public void setPreparedStatementValue(
      DatabaseMeta databaseMeta, PreparedStatement preparedStatement, int index, Object data)
      throws HopDatabaseException {
    try {
      UUID u = (UUID) convertData(this, data);
      if (u == null) {
        preparedStatement.setNull(index, Types.OTHER);
        return;
      }

      // Optimistic try: supposes the user uses uuid ONLY if the database supports it
      try {
        preparedStatement.setObject(index, u);
        return;
      } catch (Exception ignore) {
        // fall through to string fallback
      }

      // generic fallback to String
      preparedStatement.setString(index, u.toString());
    } catch (Exception e) {
      throw new HopDatabaseException("Unable to set UUID parameter", e);
    }
  }

  @Override
  public Object getValueFromResultSet(IDatabase iDatabase, ResultSet resultSet, int index)
      throws HopDatabaseException {
    try {
      Object o = resultSet.getObject(index + 1);
      return convertData(this, o);
    } catch (SQLException e) {
      throw new HopDatabaseException(
          "Unable to get value '" + toStringMeta() + "' from database resultset, index " + index,
          e);
    } catch (Exception e) {
      throw new HopDatabaseException("Unable to read UUID value", e);
    }
  }

  @Override
  public String getDatabaseColumnTypeDefinition(
      IDatabase iDatabase,
      String tk,
      String pk,
      boolean useAutoIncrement,
      boolean addFieldName,
      boolean addCr) {
    final String col = addFieldName ? getName() + " " : "";
    String def = "VARCHAR(36)";
    if (iDatabase.isPostgresVariant() || iDatabase.getPluginId().equalsIgnoreCase("H2")) {
      def = "UUID";
    } else if (iDatabase.isMsSqlServerNativeVariant()) {
      def = "UNIQUEIDENTIFIER";
    }
    return col + def + (addCr ? Const.CR : "");
  }

  @Override
  public byte[] getBinaryString(Object object) throws HopValueException {
    if (isStorageBinaryString() && identicalFormat) {
      return (byte[]) object;
    }
    UUID u = (UUID) convertData(this, object);
    if (u == null) {
      return null;
    }

    try {
      return u.toString().getBytes(getStringEncoding() == null ? "UTF-8" : getStringEncoding());
    } catch (UnsupportedEncodingException e) {
      throw new HopValueException("Unsupported encoding for UUID", e);
    } catch (Exception e) {
      throw new HopValueException("Unable to get binary string for UUID", e);
    }
  }

  @Override
  public void writeData(DataOutputStream outputStream, Object object) throws HopFileException {
    try {
      outputStream.writeBoolean(object == null);

      if (object != null) {
        UUID u = (UUID) convertData(this, object);
        byte[] b = getBinaryString(u);
        outputStream.writeInt(b.length);
        outputStream.write(b);
      }
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to write value data to output stream", e);
    } catch (Exception e) {
      throw new HopFileException(
          "Unable to convert data to UUID before writing to output stream", e);
    }
  }

  @Override
  public Object readData(DataInputStream inputStream)
      throws HopFileException, SocketTimeoutException {
    try {
      // Is the value NULL?
      if (inputStream.readBoolean()) {
        return null;
      }

      int inputLength = inputStream.readInt();
      if (inputLength < 0) {
        return null;
      }
      byte[] b = new byte[inputLength];
      inputStream.readFully(b);
      return convertBinaryStringToNativeType(b);

    } catch (EOFException e) {
      throw new HopEofException(e);
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (IOException e) {
      throw new HopFileException(this + " : Unable to read UUID value data from input stream", e);
    } catch (Exception e) {
      throw new HopFileException("Error reading UUID", e);
    }
  }
}
