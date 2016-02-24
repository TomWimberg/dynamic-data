package tom.dynamicdatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * This class is the entry into the dynamic database library.
 * <p>
 * Use the static factory method Connect to get an instance of Database.
 * <p>
 * Use createType to create a new type for a data object and
 * createProperty to create properties for types.
 * <p>
 * Use the methods createDataObject to create new data objects based
 * on a type or
 * use fetchDataObjects or fetchDataObjectBYID 
 * to fetch data objects from the database.
 * <p>
 * The methods commit and rollback are used to commit or rollback the
 * current transaction.  Note that the database is always in a 
 * transaction and these methods must be used to commit or rollback
 * the transaction.  If commit is not used, then data will not
 * be persisted.
 * <p>
 * The reset method can be used to reinitialize a database.
 * <p>
 * When done, use the close method to close the database.
 * 
 * @author Tom Wimberg
 *
 */
public class Database implements AutoCloseable {
	
	private static final String GET_NEXT_SEQUENCE_VALUE =
		"select data_sequence.nextval from dual";
	private static final String INSERT_OBJECT_SQL = 
		"INSERT INTO object (id, metadata_id) VALUES (?, ?)";
	private static final String INSERT_PROPERTY_STRING_SQL =
		"INSERT INTO value_string (object_id, metadata_id, data) VALUES (?, ?, ?)";
	private static final String INSERT_PROPERTY_OBJECT_SQL =
		"INSERT INTO value_object (object_id, metadata_id, data) VALUES (?, ?, ?)";
	private static final String SELECT_OBJECT_BASE_SQL =
		"SELECT o.id FROM object o ";
	private static final String SELECT_OBJECT_STRING_JOIN_SQL =
		"JOIN value_string p%d ON p%d.object_id = o.id AND p%d.metadata_id = ? AND p%d.data = ? ";
	private static final String SELECT_OBJECT_OBJECT_JOIN_SQL =
		"JOIN value_object p%d ON p%d.object_id = o.id AND p%d.metadata_id = ? AND p%d.data = ? ";
	private static final String SELECT_STRING_VALUE_SQL =
		"SELECT object_id, metadata_id, data FROM value_string WHERE object_id IN (%s)";
	private static final String SELECT_OBJECT_VALUE_SQL =
		"SELECT object_id, metadata_id, data FROM value_object WHERE object_id IN (%s)";
	private static final String SELECT_OBJECT_BY_ID =
		"SELECT metadata_id FROM object WHERE id = ?";
	private static final String SELECT_PROPERTY_STRING_SQL_BY_ID =
		"SELECT metadata_id, data FROM value_string WHERE object_id = ?";
	private static final String SELECT_PROPERTY_OBJECT_SQL_BY_ID =
		"SELECT metadata_id, data FROM value_object WHERE object_id = ?";
	private static final String DELETE_OBJECT_BY_ID_SQL =
		"DELETE FROM object WHERE id = ?";
	private static final String DELETE_VALUE_STRING_BY_ID_SQL =
		"DELETE FROM value_string WHERE object_id = ?";
	private static final String DELETE_VALUE_OBJECT_BY_ID_SQL = 
		"DELETE FROM value_object WHERE object_id = ?";

	// Object variables
	private Connection con = null;
	private TypeSystem typeSystem = null;

	/**
	 * Connect to a dynamic database - this factory method connects to the database,
	 * optionally resets the database, and initializes the type system.
	 * <p>
	 * If reset is set, the previous contents of the database are removed
	 * @param driverClassName	The JDBC database driver class name to use
	 * @param dbUrl				The JDBC database URL to use
	 * @param dbUserName		The JDBC database username to use
	 * @param dbPassword		The JDBC database password to use
	 * @param reset				Whether or not to reset the database
	 * @return					The database object
	 * @throws DBException
	 */
	public static Database connect(String driverClassName, 
			String dbUrl, String dbUserName, String dbPassword, boolean reset) throws DBException {
		Database db = new Database();
		try {
			
			// Load JDBC driver
			Class.forName(driverClassName);
			
			// Connect to the database
			db.con = DriverManager.getConnection(dbUrl, dbUserName, dbPassword);
			db.con.setAutoCommit(false);
			
			// If needed, reset the database
			if (reset) {
				InitializeDatabase.reset(db.con);
			}
			
			// Get a type system
			db.typeSystem = new TypeSystem(db);
			
			// Read in user-defined types
			db.typeSystem.readUserTypes();
			
		} catch (Exception e) {
			if (db.con != null) {
				db.close();
			}
			throw new DBException("Exception in connecting to database", e);
		}
		return db;
	}
	
	/*
	 * Private constructor - make people use factory method
	 */
	private Database() {}
	
	/**
	 * Reset the database to the newly created state
	 * @return					True if success, false if not
	 * @throws DBException
	 */
	public void reset() throws DBException {
		if (con == null) {
			throw new DBException("Connection closed");
		}
		InitializeDatabase.reset(con);
	}
	
	/**
	 * Get all type names currently legal in the database
	 * @return		A list of all type names
	 */
	public List<String> getTypeNames() {
		return typeSystem.getTypeList()
				.stream()
				.map(to -> to.getString(TypeSystem.TYPE_PROPERTY_NAME))
				.collect(Collectors.toList());
	}
	
	/**
	 * Get information about properties for a type
	 * @param typeName	The type name
	 * @return			A list of PropertyInfo objects
	 */
	public List<PropertyInfo> getPropertiesForType(String typeName) {
		return typeSystem.getPropertyListByTypeName(typeName)
				.stream()
				.map(po -> new PropertyInfo(po.getString(TypeSystem.PROPERTY_PROPERTY_NAME),
											po.getString(TypeSystem.PROPERTY_PROPERTY_TYPE)))
				.collect(Collectors.toList());
	}

	/**
	 * Create a new database object
	 * @param typeName			The name of the database object type
	 * @return					The type or null if no such type name
	 * @throws DBException
	 */
	public DataObject createDataObject(String typeName) throws DBException {

		if (con == null) {
			throw new DBException("Connection closed");
		}

		DataObject typeObject = typeSystem.getTypeByName(typeName);
		if (typeObject == null) {
			throw new DBException("No such type: " + typeName);
		}
		DataObject newObject = new DataObject(this, typeSystem, typeObject, false);
		return newObject;
	}
	
	/**
	 * Fetch a list of database objects by key values
	 * @param key				A DataObject with values partially filled in
	 * @return					A List of DataObject objects that match the provided keys
	 * @throws DBException
	 */
	public List<DataObject> fetchDataObjects(DataObject key) throws DBException {
		
		if (con == null) {
			throw new DBException("Connection closed");
		}

		// Set up basic query and value list
		StringBuffer objectQuerySQL = new StringBuffer(SELECT_OBJECT_BASE_SQL);
		List<Object> objectQueryValueList = new ArrayList<>();
		int joinIndex = 1;

		// Add joins for all String values in the key object
		for (Entry<Long, String> valueEntry : key.getStringProperties()) {
			objectQuerySQL.append(String.format(SELECT_OBJECT_STRING_JOIN_SQL, joinIndex, joinIndex, joinIndex, joinIndex));
			objectQueryValueList.add(valueEntry.getKey());
			objectQueryValueList.add(valueEntry.getValue());
			joinIndex++;
		}
		
		// Add joins for all Object values in the key object
		for (Entry<Long, Long> valueEntry : key.getObjectIdProperties()) {
			objectQuerySQL.append(String.format(SELECT_OBJECT_OBJECT_JOIN_SQL, joinIndex, joinIndex, joinIndex, joinIndex));
			objectQueryValueList.add(valueEntry.getKey());
			objectQueryValueList.add(valueEntry.getValue());
			joinIndex++;
		}

		// Add the type metadata
		objectQuerySQL.append("WHERE o.metadata_id = ? ");
		objectQueryValueList.add(key.getTypeId());
		
		// Set up object map by object ID
		Map<Long, DataObject> dataObjectMap = new HashMap<>();
		
		// Fetch the string values
		String stringValuesQuerySql = String.format(SELECT_STRING_VALUE_SQL, objectQuerySQL);
		
		try (PreparedStatement ps = con.prepareStatement(stringValuesQuerySql)) {
			setPsParams(ps, objectQueryValueList);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					DataObject dataObject = getOrCreateObject(dataObjectMap, rs.getLong(1), key);
					dataObject.setString(rs.getLong(2), rs.getString(3));
				}
			}
		} catch (SQLException e) {
			throw new DBException ("Exception on fetch string values", e);
		}
		
		// Fetch object values
		String objectIdValuesQuerySql = String.format(SELECT_OBJECT_VALUE_SQL, objectQuerySQL);
		try (PreparedStatement ps = con.prepareStatement(objectIdValuesQuerySql)) {
			setPsParams(ps, objectQueryValueList);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					DataObject dataObject = getOrCreateObject(dataObjectMap, rs.getLong(1), key);
					dataObject.setObjectId(rs.getLong(2), rs.getLong(3));
				}
			}
		} catch (SQLException e) {
			throw new DBException("Exception on fetch string values", e);
		}
		
		return new ArrayList<DataObject>(dataObjectMap.values());
	}
	
	/*
	 * Common code to set parameters to fetch data
	 */
	private void setPsParams(PreparedStatement ps, List<Object> valueList) throws SQLException {
		int index = 1;
		for (Object o : valueList) {
			if (o instanceof String) {
				ps.setString(index++, (String)o);
			} else if (o instanceof Long) {
				ps.setLong(index++, (Long)o);
			} else {
				throw new SQLException("Unknown value in value list");
			}
		}
	}
	
	/*
	 * Common code to create a data object on the first data value found
	 */
	private DataObject getOrCreateObject(Map<Long, DataObject> dataObjectMap, long objectId, DataObject key) {
		DataObject dataObject = dataObjectMap.get(objectId);
		if (dataObject == null) {
			dataObject = new DataObject(this, this.typeSystem, key.getType(), true);
			dataObject.setId(objectId);
			dataObjectMap.put(objectId, dataObject);
		}
		return dataObject;
	}
	
	/**
	 * Fetch a data object from the database using the object id.
	 * @param objectId			The object id to fetch
	 * @return					The DataObject or null if no such object or error
	 * @throws DBException
	 */
	public DataObject fetchDataObjectById(long objectId) throws DBException {

		if (con == null) {
			throw new DBException("Connection closed");
		}

		DataObject dataObject = null;
		
		// Fetch object to get metadata ID - just in case there are no properties for this object
		try (PreparedStatement ps = con.prepareStatement(SELECT_OBJECT_BY_ID)) {
			ps.setLong(1, objectId);
			try (ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) return null; // Nothing returned
				long typeId = rs.getLong(1);
				DataObject typeObject = typeSystem.getTypeByID(typeId);
				if (typeObject == null) {
					System.err.println("Illegal type retrieved from database: " + typeId);
					return null;
				}
				dataObject = new DataObject(this, typeSystem, typeObject, true);
				dataObject.setId(objectId);
			}
		} catch (SQLException e) {
			throw new DBException("Exception on fetch object by id", e);
		}
		
		// Get the string properties
		try (PreparedStatement ps = con.prepareStatement(SELECT_PROPERTY_STRING_SQL_BY_ID)) {
			ps.setLong(1, objectId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					dataObject.setString(rs.getLong(1), rs.getString(2));
				}
			}
		} catch (SQLException e) {
			throw new DBException("Exception on fetch string values for object by id", e);
		}
		
		// Get the object properties
		try (PreparedStatement ps = con.prepareStatement(SELECT_PROPERTY_OBJECT_SQL_BY_ID)) {
			ps.setLong(1, objectId);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					dataObject.setObjectId(rs.getLong(1), rs.getLong(2));
				}
			}
		} catch (SQLException e) {
			throw new DBException("Exception on fetch object ID values for object by id", e);
		}
		
		return dataObject;
		
	}
	
	/**
	 * Store an object - this is for use by DataObject
	 * @param object		The object to store
	 * @return				True if successful, false otherwise
	 * @throws DBException
	 */
	protected void storeObject(DataObject object) throws DBException {
		
		if (con == null) {
			throw new DBException("Connection closed");
		}

		DataObject type = object.getType();
		try (PreparedStatement objectPS = con.prepareStatement (INSERT_OBJECT_SQL)) {
			long objectId = getNextSequenceNumber(con);
			object.setId(objectId);
			objectPS.setLong(1, objectId);
			objectPS.setLong(2, object.getType().getId());
			objectPS.execute();
		} catch (SQLException e) {
			throw new DBException("Exception on store object", e);
		}
		
		List<DataObject> propertyList = typeSystem.getPropertyListByTypeID(type.getId());
		for (DataObject property : propertyList) {			
			
			String dataType = property.getString(TypeSystem.PROPERTY_PROPERTY_TYPE_ID);
			if (dataType.equals(TypeSystem.DATATYPE_STRING)) {
				
				// Handle String values

				String value = object.getString(property.getId());
				if (value == null) {
					continue; // Skip any missing values - TODO add not null constraint
				}
				
				try (PreparedStatement valuePs = con.prepareStatement(INSERT_PROPERTY_STRING_SQL)) {
					valuePs.setLong(1, object.getId());
					valuePs.setLong(2, property.getId());
					valuePs.setString(3, value);
					valuePs.execute();
				} catch (SQLException e) {
					throw new DBException("Exception on store String value", e);
				}
				
			} else {
				
				// Handle Object values
				
				Long value = object.getObjectId(property.getId());
				if (value == null) {
					continue; // Skip any missing values - TODO add not null constraint
				}
					
				try (PreparedStatement valuePs = con.prepareStatement(INSERT_PROPERTY_OBJECT_SQL)){
					
					valuePs.setLong(1, object.getId());
					valuePs.setLong(2, property.getId());
					valuePs.setLong(3, value);
					valuePs.execute();
				} catch (SQLException e) {
					throw new DBException("Exception on store String value", e);
				}
			}
		}		
	}
	
	/*
	 * Get the next sequence number
	 */
	private long getNextSequenceNumber(Connection con) throws SQLException, DBException {
		
		try (
				PreparedStatement ps = con.prepareStatement(GET_NEXT_SEQUENCE_VALUE);
				ResultSet rs = ps.executeQuery();
			) {
			if (!rs.next()) {
				throw new DBException("No sequence data returned");
			}
			return rs.getLong(1);
		}
	}
	
	/*
	 * Delete an object
	 * @param object	The object to be deleted
	 * @return
	 * @throws DBException
	 */
	 void deleteObject(DataObject object) throws DBException {
		
		if (con == null) {
			throw new DBException("Connection closed");
		}

		long objectId = object.getId();
		
		try (
				PreparedStatement objectPs = con.prepareStatement(DELETE_OBJECT_BY_ID_SQL);
				PreparedStatement propertyStringPs = con.prepareStatement(DELETE_VALUE_STRING_BY_ID_SQL);
				PreparedStatement propertyObjectPs = con.prepareStatement(DELETE_VALUE_OBJECT_BY_ID_SQL);
				) {
			
			// Delete any string properties
			propertyStringPs.setLong(1, objectId);
			propertyStringPs.execute();
			
			// Delete any object properties
			propertyObjectPs.setLong(1, objectId);
			propertyObjectPs.execute();
			
			// Delete the object - this must be done last so key constraints are not violated
			objectPs.setLong(1, objectId);
			objectPs.execute();

		} catch (SQLException e) {
			throw new DBException("Exception on deleting object", e);
		}
	}

	/**
	 * Commit the current transaction
	 * <p>
	 * Note that the database is not run in auto-commit mode.
	 * @return					True if successful, false if not
	 * @throws DBException
	 */
	public void commit() throws DBException {

		if (con == null) {
			throw new DBException("Connection closed");
		}

		try {
			con.commit();
		} catch (SQLException e) {
			throw new DBException("Commit failed", e);
		}
	}
	
	/**
	 * Rollback the current transaction
	 * <p>
	 * Note that the database is not run in auto-commit mode
	 * @return					True if successful, false if not
	 * @throws DBException
	 */
	public void rollback() throws DBException {
		
		if (con == null) {
			throw new DBException("Connection closed");
		}

		try {
			con.rollback();
		} catch (SQLException e) {
			throw new DBException("Rollback failed", e);
		}
	}
	
	/**
	 * Close the database
	 * @return					True if successful, false if not
	 * @throws DBException
	 */
	public void close() throws DBException {
		
		if (con == null) {
			throw new DBException("Connection closed");
		}

		try {
			con.close();
			con = null;
		} catch (SQLException e) {
			throw new DBException("Database close failed", e);
		}
	}
	
	/**
	 * Dump the database to sysout for diagnostics
	 * @throws DBException
	 */
	public void dump() throws DBException{
		
		if (con == null) {
			throw new DBException("Connection closed");
		}

		try (PreparedStatement ps = con.prepareStatement(
				"SELECT id, metadata_id FROM object ORDER BY id, metadata_id")) {
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					System.out.printf("id: %d, metadata_id: %d\n",
							rs.getLong(1), rs.getLong(2));
				}
			}
			
		} catch (SQLException e) {
			throw new DBException ("Exception on object dump", e);
		}
		
		try (PreparedStatement ps = con.prepareStatement(
				"SELECT object_id, metadata_id, data FROM value_string ORDER BY object_id, metadata_id")){
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					System.out.printf("object_id: %d, metadata_id: %d, value %s\n",
							rs.getLong(1), rs.getLong(2), rs.getString(3));
				}
			}
		} catch (SQLException e) {
			throw new DBException("Exception on string value dump", e);
		}
		
		try (PreparedStatement ps = con.prepareStatement(
				"SELECT object_id, metadata_id, data FROM value_object ORDER BY object_id, metadata_id")) {
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					System.out.printf("object_id: %d, metadata_id: %d, value %d\n",
							rs.getLong(1), rs.getLong(2), rs.getLong(3));
				}
			}
		} catch (SQLException e) {
			throw new DBException("Exception on object id value dump", e);
		}
	}
	

	/**
	 * Create a Type object and persist it.  This is a convenience method.
	 * <p>
	 * This should be in the TypeSystem class, but there is no good way to get to the
	 * type system object at this time.
	 * @param typeName			The name of the new type
	 * @return					The newly created Type object
	 * @throws DBException
	 */
	public DataObject createType(String typeName) throws DBException {
		DataObject newType = createDataObject(TypeSystem.TYPE_TYPE_NAME);
		newType.setString(TypeSystem.TYPE_PROPERTY_NAME, typeName);
		newType.persist(); // To get type ID
		return newType;
	}
	
	/**
	 * Create a Property object and persist it.  This is a convenience method.
	 * <p>
	 * This should be in the TypeSystem class, but there is no good way to get to the
	 * type system object at this time.
	 * @param typeObject		The type object for which to create the property
	 * @param propertyName		The name of the new property
	 * @param dataType			The data type - "String" or a Type name
	 * @return					The newly create Property object
	 * @throws DBException
	 */
	public DataObject createProperty(DataObject typeObject, 
			String propertyName, String dataType) throws DBException {
		DataObject newProperty = createDataObject(TypeSystem.PROPERTY_TYPE_NAME);
		newProperty.setObject(TypeSystem.PROPERTY_PROPERTY_OWNER, typeObject);
		newProperty.setString(TypeSystem.PROPERTY_PROPERTY_NAME, propertyName);
		newProperty.setString(TypeSystem.PROPERTY_PROPERTY_TYPE, dataType);
		newProperty.persist();
		return newProperty;
	}

}
