package tom.dynamicdatabase;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * The object layer of the dynamic database library
 * <p>
 * Objects are obtained from the Database object - for new ones and ones
 * read from the database.
 * <p>
 * Once you have a database object you can:
 * <ul>
 * <li>Get the type id, type name, or type object using the getTypeId,
 * getTypeName, and getType methods
 * <li>Get the object id (for objects that have been read from the database
 * or that have been persisted) using the getId method
 * <li>Set values in an object using the setString and setObject methods
 * <li>Get values in an object using the getString, getObjectId, and getObject methods
 * <li>Persist an object using the persist method
 * <li>Delete an object using the delete method
 * <li>Create Type and Property Object using create
 * <el>
 * 
 * @author Tom Wimberg
 *
 */
public class DataObject {
	
	// Data about the object
	private long id;						// The object ID - assigned when persisted
	private long typeId;					// The object type ID
	private String typeName;				// The object type name
	private DataObject typeObject;			// The object type object
	private Database db;					// The database to which this object is attached
	private TypeSystem typeSystem;			// The type system to which this object is attached
	private boolean dbObject = false;		// Is this object in the database?
	
	// Maps of String values
	private Map<Long, String> stringValueMap = new HashMap<>();
	private Set<Entry<Long, String>> stringValueEntries = stringValueMap.entrySet();
	
	// Maps of Object ID values
	private Map<Long, Long> objectIdValueMap = new HashMap<>();
	private Set<Entry<Long, Long>> objectIdValueEntries = objectIdValueMap.entrySet();
	
	// Keep track of which values have been updated - populated lazily - false assumed
	private Map<Long, Boolean> modifiedValuedMap = new HashMap<>();
	
	// These value maps are populated lazily when requested (for the most part)
	private Map<Long, DataObject> objectValueMap = new HashMap<>();
	
	/**
	 * Constructor - for use by the DB and type system
	 */
	DataObject(Database db, TypeSystem typeSystem, DataObject typeObject, boolean dbObject) {
		this.db = db;
		this.typeSystem = typeSystem;
		if (typeObject != null) {
			// Will be null in bootstrap
			this.typeObject = typeObject;
			this.typeId = typeObject.getId();
			this.typeName = typeObject.getString(TypeSystem.TYPE_PROPERTY_NAME_ID);
		}
		this.dbObject = dbObject;
	}
	
	/*
	 * Set type info for bootstrap object creation
	 */
	void setTypeInfo(long typeId, String typeName, DataObject typeObject) {
		this.typeId = typeId;
		this.typeName = typeName;
		this.typeObject = typeObject;
	}
	
	/**
	 * Get the object's type ID for this object
	 * @return
	 */
	public long getTypeId() {
		return typeId;
	}
	
	/**
	 * Get the object's type name
	 * @return
	 */
	public String getTypeName() {
		return typeName;
	}
	
	/**
	 * Get the type object for this object
	 * @return
	 */
	public DataObject getType () {
		return typeObject;
	}
	
	/**
	 * Set the ID for this object - only for bootstrap
	 * @param id
	 */
	void setId(long id) {
		this.id = id;
	}
	
	/**
	 * Get the object ID
	 * @return
	 */
	public long getId() {
		return id;
	}
	
	/*
	 * Set a string value - for db and type system only - this is not checked for valid types
	 */
	void setString(long id, String value) {
		stringValueMap.put(id, value);
		modifiedValuedMap.put(id, true);
	}
	
	/**
	 * Set a String value by name - this checks for valid property name and data type
	 * @param propertyName			The name of the value
	 * @param value				The value - can be null (null values are not stored)
	 * @return					True if successful, false if no such property name or wrong data type
	 * @throws DBException
	 */
	public void setString(String propertyName, String value) throws DBException {
		
		// Make sure this is a valid property
		DataObject property = typeSystem.getPropertyByNames(typeName, propertyName);
		if (property == null) {
			throw new DBException("No such property");
		}
		
		// Make sure the data type is correct
		if (!property.getString(TypeSystem.PROPERTY_PROPERTY_TYPE_ID).equals(TypeSystem.DATATYPE_STRING)) {
			throw new DBException("Data type mismatch");
		}
		if (typeId == TypeSystem.PROPERTY_TYPE_ID && property.getId() == TypeSystem.PROPERTY_PROPERTY_TYPE_ID) {
			// This is setting a data type for a property - test for valid values - TODO - do this cleanly
			if (!typeSystem.checkDataType(value)) {
				throw new DBException("Unknown data type");
			}
		}
		
		// Only set data if value is not null
		if (value != null) {
			setString(property.getId(), value);			
		}
		
	}
	
	/*
	 * Set a string value by property ID - internal only - this does not check valid properties and types 
	 */
	void setString(Long propertyId, String value) throws DBException {
		
		// Only save value if it is not null
		if (value != null) {
			setString(propertyId, value);			
		}
	}
	
	/*
	 * Set an object - id and object - internal only - this is not checked for valid types
	 */
	void setObject(long propertyId, long valueId, DataObject value) {
		objectIdValueMap.put(propertyId, valueId);
		objectValueMap.put(propertyId, value);
		modifiedValuedMap.put(propertyId, true);
	}

	/**
	 * Set an object property value by property name - this checks for legal property name and value type
	 * @param propertyName		The property name to set
	 * @param value				The property value - can be null (null values are not stored)
	 * @return					True if successful, false if no such property name or wrong data type
	 * @throws DBException
	 */
	public void setObject(String propertyName, DataObject value) throws DBException {
		
		// Check for correct property type
		DataObject property = typeSystem.getPropertyByNames(typeName, propertyName);
		if (property == null) {
			throw new DBException("No such property");
		}

		// Only set value if it is not null
		if (value != null) {
			
			if (!property.getString(TypeSystem.PROPERTY_PROPERTY_TYPE_ID).equals(value.typeName)) {
				throw new DBException("Data type mismatch");
			}
			
			setObject(property.getId(), value.getId(), value);
		}
	}
	
	/*
	 * Set an object property value by property ID - this checks for legal property ID and value type
	 */
	void setObject(long propertyId, DataObject value) throws DBException {
		
		// Check for correct property type
		DataObject property = typeSystem.getPropertyByIds(typeId, propertyId);
		if (property == null) {
			throw new DBException("No such property");
		}

		// Only set data if not null
		if (value != null) {

			if (!property.getString(TypeSystem.PROPERTY_PROPERTY_TYPE_ID).equals(value.typeName)) {
				throw new DBException("Data type mismatch");
			}

			setObject(property.getId(), value.getId(), value);
		}
	}

	/*
	 * Set object ID by property ID - used by db only
	 */
	void setObjectId(long propertyId, long valueId) {
		objectIdValueMap.put(propertyId, valueId);
		modifiedValuedMap.put(propertyId, true);
	}
	
	/*
	 * Return a string value by property ID - internal use only - no checks
	 */
	String getString(Long propertyId) {
		return stringValueMap.get(propertyId);
	}
	
	/**
	 * Return a String value by property name
	 * @param propertyName		The name of the property
	 * @return					The value or null if no value
	 * @throws DBException
	 */
	public String getString(String propertyName) throws DBException {

		// Make sure this is a valid property
		DataObject property = typeSystem.getPropertyByNames(typeName, propertyName);
		if (property == null) {
			throw new DBException("No such property");
		}

		return getString(property.getId());

	}
	
	/*
	 * Get an object ID by property ID - internal use only - no checks
	 */
	Long getObjectId(long propertyId) {
		return objectIdValueMap.get(propertyId);
	}
	
	/**
	 * Get an object ID by property name
	 * @param propertyName		The name of the property
	 * @return					The object ID or null if no object value by that name
	 * @throws DBException
	 */
	public Long getObjectId(String propertyName) throws DBException {
		
		// Make sure this is a valid property
		DataObject property = typeSystem.getPropertyByNames(typeName, propertyName);
		if (property == null) {
			throw new DBException("No such property");
		}
		
		return getObjectId(property.getId());
	}
	
	/**
	 * Get an object value by property id - internal use only - minimal checks
	 */
	DataObject getObject(long propertyId) throws DBException {

		// See if object ID value exists
		Long objectId = getObjectId(propertyId);
		if (objectId == null) {
			return null;
		}
		
		// Object value exists - fetch if we don't already have
		DataObject returnObject = objectValueMap.get(propertyId);
		if (returnObject == null) {
			// Get object and save it
			returnObject = db.fetchDataObjectById(objectId);
			setObject(propertyId, returnObject);
		}
		
		return returnObject;
	}
	
	/**
	 * Get an object value by property name - this may cause a database fetch
	 * <p>
	 * Property names are not checked for validity - a bad property value will
	 * return a null value
	 * @param propertyName		The name of the property
	 * @return					The object or null if no object
	 * @throws DBException
	 */
	public DataObject getObject(String propertyName) throws DBException {

		// Make sure this is a valid property
		DataObject property = typeSystem.getPropertyByNames(typeName, propertyName);
		if (property == null) {
			throw new DBException("No such property");
		}
		
		return getObject(property.getId());
	}
	
	/*
	 * Return all the string property IDs and values for an object - db only
	 */
	Set<Entry<Long, String>> getStringProperties() {
		 return stringValueEntries;
	}
	
	/*
	 * Return all the object ID property IDs and values for an object - db only
	 */
	Set<Entry<Long, Long>> getObjectIdProperties() {
		return objectIdValueEntries;
	}
	
	/**
	 * Persist a data object - insert or update based on whether or not object came from database
	 * @return		True if successful, false if not
	 * @throws DBException
	 */
	public void persist() throws DBException {
		
		// Don't touch base meta-data
		if (id > 0 && id < 100) {
			throw new DBException("Attempt to persist built-in type or property");
		}
		
		// Is this object alread in the database?
		if (dbObject) {
			// If so, update the object
			db.updateObject(this);
		} else {			
			// Store object 		
			db.storeObject(this);
		}
		
		// Mark object as being in database
		dbObject = true;
		
		// Check for type definitions
		if (typeId == TypeSystem.TYPE_TYPE_ID) {
			typeSystem.addType(this);
		}
		
		// Check for property definitions
		if (typeId == TypeSystem.PROPERTY_TYPE_ID) {
			typeSystem.addProperty(this);
		}
		
		// Delete modified map - all values have been persisted
		modifiedValuedMap = new HashMap<>();
	}
	
	/*
	 * Return whether or not a property has been modified since last persisted.
	 * This is for internal use only.
	 */
	boolean isModified(long propertyId) {
		Boolean returnValue = this.modifiedValuedMap.get(propertyId);
		if (returnValue == null) {
			return false;
		}
		return returnValue;
	}
	
	/**
	 * Return whether or not a property value has been modified since last persisted.
	 * @param propertyName
	 * @return
	 * @throws DBException
	 */
	public boolean isModified(String propertyName) throws DBException {

		// Make sure this is a valid property
		DataObject property = typeSystem.getPropertyByNames(typeName, propertyName);
		if (property == null) {
			throw new DBException("No such property");
		}
		
		return isModified(property.getId());
	}
	
	/**
	 * Delete an object
	 * @return
	 * @throws DBException
	 */
	public void delete() throws DBException {
		
		// Don't delete base metadata
		if (id < 100) {
			throw new DBException("Attempt to delete built-in type or property");
		}
		
		// Mark object as not in database
		dbObject = false;
		
		// Delete it
		db.deleteObject(this);
	}
	
}
