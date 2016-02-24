package tom.dynamicdatabase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to handle the type system
 * <p>
 * The type system is a set of objects of the type Type and type Property.
 * These are represented by DataObject objects.
 * <p>
 * The Type type has one property - Name
 * <p>
 * The Property type has three properties:
 * <ul>
 * <li>Owner - an object reference to the Type object to which this property belongs
 * <li>Name - the name of the property
 * <li>Type - the data type of the property - String or another Type
 * </ul>
 * <p>
 * There are public constants for the Type type and the Prototype type along with a constant
 * for the String datatype.
 * <p>
 * There are no public methods in this class.
 * 
 * @author Tom Wimberg
 *
 */
public class TypeSystem {

	// The metadata properties for type
	/**
	 * Type object type name - used as data type for new types
	 */
	public static final String TYPE_TYPE_NAME = "Type";
	static final long TYPE_TYPE_ID = 1;
	/**
	 * Type object name property name - used when creating new types
	 */
	public static final String TYPE_PROPERTY_NAME = "Name";
	static final long TYPE_PROPERTY_NAME_ID = 5;

	// The metadata for Property
	/**
	 * Property object type name - used when creating new properties
	 */
	public static final String PROPERTY_TYPE_NAME = "Property";
	static final long PROPERTY_TYPE_ID = 2;

	/**
	 * Property object owner property name - used when creating new properties
	 */
	public static final String PROPERTY_PROPERTY_OWNER = "Owner";
	static final long PROPERTY_PROPERTY_OWNER_ID = 6;

	/**
	 * Property object name property - used when creating new properties
	 */
	public static final String PROPERTY_PROPERTY_NAME = "Name";
	static final long PROPERTY_PROPERTY_NAME_ID = 7;
	
	/**
	 * Property object type property - used when creating new properties
	 */
	public static final String PROPERTY_PROPERTY_TYPE = "Type";
	static final long PROPERTY_PROPERTY_TYPE_ID = 8;

	// Values for datatypes - plus type names
	/**
	 * The String data type name
	 */
	public static final String DATATYPE_STRING = "String";

	// Information about all known types
	private List<DataObject> typeList = new ArrayList<DataObject>();
	private Map<String, DataObject> typeMapByName = new HashMap<String, DataObject>();
	private Map<Long, DataObject> typeMapByMetadataID = new HashMap<Long, DataObject>();
	
	// Information about all known properties
	private Map<String, List<DataObject>> propertyListForTypeByTypeNameMap = new HashMap<String, List<DataObject>>();
	private Map<Long, List<DataObject>> propertyListForTypeByTypeIDMap = new HashMap<Long, List<DataObject>>();
	private Map<String, Map<String, DataObject>> propertyMapByTypePropertyName = new HashMap<String, Map<String, DataObject>>();
	private Map<Long, Map<Long, DataObject>> propertyMapByTypePropertyIds =	new HashMap<Long, Map<Long, DataObject>>();
	
	// The database object
	private Database db = null;
	
	/*
	 * Return the list of all known types
	 */
	List<DataObject>getTypeList() {
		return typeList;
	}
	
	/*
	 * Method to return a type object by name
	 */
	DataObject getTypeByName(String name) {
		return typeMapByName.get(name);
	}
	
	/*
	 * Method to return a type's metadata by metadata ID
	 */
	DataObject getTypeByID(long id) {
		return typeMapByMetadataID.get(id);
	}
	
	/*
	 * This method is called whenever a new type is defined - used by db
	 */
	void addType(DataObject type) {
		addType(type, type.getString(TYPE_PROPERTY_NAME_ID), type.getId());
	}
	
	/*
	 * This method adds a new type - used by bootstrap
	 */
	void addType(DataObject type, String typeName, long typeId) {
		typeList.add(type);
		typeMapByMetadataID.put(typeId, type);
		typeMapByName.put(typeName, type);
	}
	
	/* 
	 * Get a list of properties for a type by the type ID
	 */
	List<DataObject> getPropertyListByTypeID(long typeID) {
		List<DataObject> returnValue = propertyListForTypeByTypeIDMap.get(typeID);
		if (returnValue == null) {
			returnValue = new ArrayList<>();
		}
		return propertyListForTypeByTypeIDMap.get(typeID);
	}
	
	/*
	 * Get a list of properties for a type by the type name
	 */
	List<DataObject> getPropertyListByTypeName(String typeName) {
		return propertyListForTypeByTypeNameMap.get(typeName);
	}
	
	/*
	 * Get property by IDs of type and property
	 * @param typeID
	 * @param propertyID
	 * @return
	 */
	DataObject getPropertyByIds(long typeID, long propertyID) {
		Map<Long, DataObject> propertyMap = propertyMapByTypePropertyIds.get(typeID);
		if (propertyMap == null) {
			return null;
		}
		return propertyMap.get(propertyID);
	}
	
	/*
	 * Get a property by the type and property name
	 * @param typeName
	 * @param propertyName
	 * @return
	 */
	DataObject getPropertyByNames(String typeName, String propertyName) {
		Map<String, DataObject> propertyMap = propertyMapByTypePropertyName.get(typeName);
		if (propertyMap == null) {
			return null;
		}
		return propertyMap.get(propertyName);
	}

	/*
	 * This method is called whenever a new property for a type is defined
	 */
	void addProperty(DataObject property) {
		// Get the type this property belongs to
		DataObject type = getTypeByID(property.getObjectId(PROPERTY_PROPERTY_OWNER_ID));
		String typeName = type.getString(TYPE_PROPERTY_NAME_ID);
		String propertyName = property.getString(PROPERTY_PROPERTY_NAME_ID);
		addProperty(type, property, typeName, propertyName);
	}
	
	/*
	 * This method is called whenever a new property for a type is defined
	 * <p>
	 * This version is used during metadata-bootstrap when the type name is not
	 * yet set for the type type.
	 */
	void addProperty(DataObject type, DataObject property, String typeName, String propertyName) {
		long typeID = type.getId();
		long propertyID = property.getId();
		
		// Add property to list of properties for a type by name and ID - share same list
		List<DataObject> propertyList = propertyListForTypeByTypeNameMap.get(typeName);
		if (propertyList == null) {
			propertyList = new ArrayList<DataObject>();
			propertyListForTypeByTypeNameMap.put(typeName, propertyList);
			propertyListForTypeByTypeIDMap.put(typeID, propertyList);
		}
		propertyList.add(property);
		
		// Add property to map of properties by names
		Map<String, DataObject> mapByName = propertyMapByTypePropertyName.get(typeName);
		if (mapByName == null) {
			mapByName = new HashMap<String, DataObject>();
			propertyMapByTypePropertyName.put(typeName, mapByName);
		}
		mapByName.put(propertyName, property);
		
		// Add property to map of properties by IDs
		Map<Long, DataObject> mapByID = propertyMapByTypePropertyIds.get(typeID);
		if (mapByID == null) {
			mapByID = new HashMap<Long, DataObject>();
			propertyMapByTypePropertyIds.put(typeID, mapByID);
		}
		mapByID.put(propertyID, property);
	}
	
	/*
	 * Check a string for a valid type name.  A valid type
	 * name is either "String" or a previously defined type name.
	 */
	boolean checkDataType(String typeString) {
		if (typeString.equals(DATATYPE_STRING)) return true;
		if (getTypeByName(typeString) != null) return true;
		return false;
	}

	/*
	 * Constructor - this initializes the type system - only used by db
	 */
	TypeSystem(Database db) {
		
		// Set db link
		this.db = db;
		
		// Create the type and property objects
		DataObject typeObject = bootstrapCreateType(TYPE_TYPE_ID, TYPE_TYPE_NAME);
		DataObject propertyObject = bootstrapCreateType(PROPERTY_TYPE_ID, PROPERTY_TYPE_NAME);
		
		// Now create the type property - name
		bootstrapCreateProperty(typeObject, TYPE_TYPE_NAME, TYPE_PROPERTY_NAME_ID, TYPE_PROPERTY_NAME, DATATYPE_STRING);
		
		// Now create the property properties - owner, name, and type
		bootstrapCreateProperty(propertyObject, PROPERTY_TYPE_NAME, PROPERTY_PROPERTY_OWNER_ID, PROPERTY_PROPERTY_OWNER, TYPE_TYPE_NAME);
		bootstrapCreateProperty(propertyObject, PROPERTY_TYPE_NAME, PROPERTY_PROPERTY_NAME_ID, PROPERTY_PROPERTY_NAME, DATATYPE_STRING);
		bootstrapCreateProperty(propertyObject, PROPERTY_TYPE_NAME, PROPERTY_PROPERTY_TYPE_ID, PROPERTY_PROPERTY_TYPE, DATATYPE_STRING);		
	}
	
	/*
	 * Create a bootstrap type
	 */
	private DataObject bootstrapCreateType(long typeId, String typeName) {
				
		// Create type object using a null type since we have not set properties for types yet
		DataObject typeObject = new DataObject(db, this, null, true);
		typeObject.setId(typeId);
		
		// Handle bootstraping the Type base type
		DataObject baseTypeType = typeId == TYPE_TYPE_ID ? typeObject : typeMapByMetadataID.get(TYPE_TYPE_ID);
		
		// Set the rest of the type information
		typeObject.setTypeInfo(TYPE_TYPE_ID, TYPE_TYPE_NAME, baseTypeType);
		typeObject.setString(TYPE_PROPERTY_NAME, TYPE_PROPERTY_NAME_ID, typeName);
		
		// Add type to type system
		addType(typeObject, typeName, typeId);
		
		return typeObject;
	}
	
	/*
	 * Create a bootstrap property
	 */
	private DataObject bootstrapCreateProperty(DataObject typeObject, String typeName, long propertyId, String propertyName, String dataType) {

		// Create property object using a null type to handle bootstrap
		DataObject propertyObject = new DataObject(db, this, null, true);
		DataObject basePropertyTypeObject = typeMapByMetadataID.get(PROPERTY_TYPE_ID);
		propertyObject.setTypeInfo(PROPERTY_TYPE_ID, PROPERTY_TYPE_NAME, basePropertyTypeObject);
		propertyObject.setId(propertyId);
		propertyObject.setObject(PROPERTY_PROPERTY_OWNER, PROPERTY_PROPERTY_OWNER_ID, typeObject.getId(), typeObject);
		propertyObject.setString(PROPERTY_PROPERTY_NAME, PROPERTY_PROPERTY_NAME_ID, propertyName);
		propertyObject.setString(PROPERTY_PROPERTY_TYPE, PROPERTY_PROPERTY_TYPE_ID, dataType);
		addProperty(typeObject, propertyObject, typeName, propertyName);
		return propertyObject;
		
	}
	
	/*
	 * Read user types during initialization - this has to be done after the TypeSystem object
	 * has been created so it can be used as part of this process.
	 */
	void readUserTypes() throws DBException {
		// Now that basic type system is initialized, read user-defined data types and properties
		DataObject typeKey = db.createDataObject(TYPE_TYPE_NAME);
		List<DataObject> typeList = db.fetchDataObjects(typeKey);
		for (DataObject dbTypeObject : typeList) {
			// Add type to type system as long as it is not one of the base types
			if (dbTypeObject.getId() > 99) {
				addType(dbTypeObject);
			}
		}
		
		DataObject propertyKey = db.createDataObject(PROPERTY_TYPE_NAME);
		List<DataObject> propertyList = db.fetchDataObjects(propertyKey);
		for (DataObject dbPropertyObject : propertyList) {
			// Add property to type system as long as it is not one of the base properties
			if (dbPropertyObject.getId() > 99) {
				addProperty(dbPropertyObject);
			}
		}
	}
}
