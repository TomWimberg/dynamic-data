package tom.dynamicdatabase;

import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is the entry to the dynamic data project
 * <p>
 * To start you:
 * <ul>
 * <li>Get a DynamicDataStore object using the connect static factory method
 * <li>Create types and properties using createType and createProperty methods (first time through)
 * <li>Get object metadata using getType and getPropertiesForType
 * <li>Create new objects using createDataObject
 * <li>Fetch objects from the data store using fetchDataObjects or fetchDataObjectById
 * <li>Manipulate data object using the DataObject methods
 * <li>Commit or rollback the database using the commit and/or rollback methods
 * <li>Close the DynamicDataStore using the close method or by using the try resource Java construct
 * </ul>
 * @author wimberg
 *
 */
public class DataStore implements AutoCloseable {
	
	Database db = null;
	TypeSystem typeSystem = null;
	
	/**
	 * Open a dynamic data store - this factory method connects to the database,
	 * optionally resets the database, and initializes the type system.
	 * <p>
	 * If reset is set, the previous contents of the database are removed
	 * @param driverClassName	The JDBC database driver class name to use
	 * @param dbUrl				The JDBC database URL to use
	 * @param dbUserName		The JDBC database username to use
	 * @param dbPassword		The JDBC database password to use
	 * @param reset				Whether or not to reset the database
	 * @return					The data store object
	 * @throws DBException
	 */
	public static DataStore open(String driverClassName, 
			String dbUrl, String dbUserName, String dbPassword, boolean reset) throws DBException {
		
		DataStore ds = new DataStore();
		
		// Create the database connector
		ds.db = Database.connect(driverClassName, dbUrl, dbUserName, dbPassword, reset);
		
		// Create the type system
		ds.typeSystem = new TypeSystem(ds);
		
		// Tell the database layer about the type system
		ds.db.setTypeSystem(ds.typeSystem);
		
		// Read in user-defined types
		ds.typeSystem.readUserTypes();

		return ds;
	}
	
	/**
	 * Close the data store
	 * @return					True if successful, false if not
	 * @throws DBException
	 */
	public void close() throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}
		
		db.close();
		db = null;
	}

	/**
	 * Commit the current transaction
	 * <p>
	 * Note that the database is not run in auto-commit mode.
	 * @return					True if successful, false if not
	 * @throws DBException
	 */
	public void commit() throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		db.commit();
	}
	
	/**
	 * Rollback the current transaction
	 * <p>
	 * Note that the database is not run in auto-commit mode
	 * @return					True if successful, false if not
	 * @throws DBException
	 */
	public void rollback() throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		db.rollback();
	}

	/**
	 * Reset the database to the newly created state
	 * @return					True if success, false if not
	 * @throws DBException
	 */
	public void reset() throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		db.reset();
	}

	/**
	 * Get all type names currently legal in the database
	 * @return		A list of all type names
	 */
	public List<String> getTypeNames() throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		return typeSystem.getTypeList()
				.stream()
				.map(to -> to.getString(TypeSystem.TYPE_PROPERTY_NAME_ID))
				.collect(Collectors.toList());
	}
	
	/**
	 * Get information about properties for a type
	 * @param typeName	The type name
	 * @return			A list of PropertyInfo objects
	 */
	public List<PropertyInfo> getPropertiesForType(String typeName) throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		return typeSystem.getPropertyListByTypeName(typeName)
				.stream()
				.map(po -> new PropertyInfo(po.getString(TypeSystem.PROPERTY_PROPERTY_NAME_ID),
											po.getString(TypeSystem.PROPERTY_PROPERTY_TYPE_ID)))
				.collect(Collectors.toList());
	}
	
	/**
	 * Create a Type object and persist it.
	 * <p>
	 * While this can be done usingcreateObject with a type of "Type"
	 * followed by setting the property of the "Type" type, it is 
	 * recommended to use this convenience method.
	 * <p>
	 * 
	 * @param typeName			The name of the new type
	 * @return					The newly created Type object
	 * @throws DBException
	 */
	public DataObject createType(String typeName) throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		DataObject newType = createDataObject(TypeSystem.TYPE_TYPE_NAME);
		newType.setString(TypeSystem.TYPE_PROPERTY_NAME, typeName);
		newType.persist(); // To get type ID
		return newType;
	}
	
	/**
	 * Create a Property object and persist it.
	 * <p>
	 * While this can be done using createObject with a type of "Property"
	 * followed by setting the properties of the "Property" type, it is
	 * recommended to use this convenience method.
	 * @param typeObject		The type object for which to create the property
	 * @param propertyName		The name of the new property
	 * @param dataType			The data type - "String" or a Type name
	 * @return					The newly create Property object
	 * @throws DBException
	 */
	public DataObject createProperty(DataObject typeObject, 
			String propertyName, String dataType) throws DBException {

		if (db == null) {
			throw new DBException("Data store closed");
		}

		DataObject newProperty = createDataObject(TypeSystem.PROPERTY_TYPE_NAME);
		newProperty.setObject(TypeSystem.PROPERTY_PROPERTY_OWNER, typeObject);
		newProperty.setString(TypeSystem.PROPERTY_PROPERTY_NAME, propertyName);
		newProperty.setString(TypeSystem.PROPERTY_PROPERTY_TYPE, dataType);
		newProperty.persist();
		return newProperty;
	}
	
	/**
	 * Create a new database object
	 * @param typeName			The name of the database object type
	 * @return					The type or null if no such type name
	 * @throws DBException
	 */
	public DataObject createDataObject(String typeName) throws DBException {

		if (db == null) {
			throw new DBException("Data store closed");
		}

		DataObject typeObject = typeSystem.getTypeByName(typeName);
		if (typeObject == null) {
			throw new DBException("No such type: " + typeName);
		}
		DataObject newObject = new DataObject(db, typeSystem, typeObject, false);

		return newObject;
	}

	/**
	 * Fetch a list of database objects by key values
	 * <p>
	 * The TODO - add more info
	 * @param key				A DataObject with values partially filled in
	 * @return					A List of DataObject objects that match the provided keys
	 * @throws DBException
	 */
	public List<DataObject> fetchDataObjects(DataObject key) throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		return db.fetchDataObjects(key);
	}
	
	/**
	 * Fetch a data object from the database using the object id.
	 * @param objectId			The object id to fetch
	 * @return					The DataObject or null if no such object or error
	 * @throws DBException
	 */
	public DataObject fetchDataObjectById(long objectId) throws DBException {
		
		if (db == null) {
			throw new DBException("Data store closed");
		}

		return db.fetchDataObjectById(objectId);
	}
	
}
