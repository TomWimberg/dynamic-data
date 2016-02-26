# dynamic-data
This project is a test project of mine that provides dynamic object creation within an SQL database.
This was originally done as a proof of concept to see if a system could be created that would allow extra data to be stored in the same transaction as normal static data.  This required that the extra data be stored dynamically in a set of SQL tables.

There is an example program in the tom.dynamicdatabase.examples package.

The general order of operations is:
* Use DataStore.open to open a data store - the current system is configured to use H2 - this returns a DataStore object
* Use createType and createProperty on the DataStore object to create new object types and properties for those object types - these return type DataObject objects and property DataObject objects.  
* Use createDataObject, fetchDataObjects, or fetchObjectById on the DataStore object to create
new objects, fetch a set of objects from the database by key, or fetch a data object by its object id - all of these methods return a DataObject object
* Use the DataObject which will have the properties that have been defined for it
  - setString/setObject/setObjectId to set the values of properties of a DataObject
  - getString/getObject/getObjectId to get values of properties of a DataObject
  - persist to store a data object to the database (this includes updates)
  - delete to delete a data object from the database 
* Use the commit and rollback methods on the DataStore object to commit and rollback the database transaction
* Use the close method on the DataStore object to close the database connection - note that the DataStore class does implement the AutoCloseable interface so that the try with resources pattern can be used

## DBException
Almost all static and object methods raise DBException if they encounter an error.

## DataStore Object
The top level object is the DataStore object.

### DataStore.open
You use the DataStore.open static factory method to get a DataStore object and establish a connection to an SQL database.  

This method takes the following parameters:
* driverClassName - String - this is the name of the JDBC driver class to use to connect to the database
* dbUrl - String - the URL to pass to the JDBC connect method
* dbUserName - String - the username to pass to the JDBC connect method
* dbPassword - String - the password to pass to the JDBC connect method
* reset - boolean - whether or not to initialize/reset the database - this must be set to true on initial opening of a database - this will create the required tables and metadata
This method returns a DataStore object.

Note that the JDBC connection is set with auto commit turned off so you have to use the commit method to make data store changes permanent.  A transaction is started when the DataStore object is created.

### close
The DataStore object implements the AutoClosable interface so it can be used with try with resources Java construct.  

You can also use the close object method to close the JDBC connection.

This method takes no parameters and returns void.

### commit
This object method commits the current transaction and starts a new one.

This method takes no parameters and returns void.

### rollback
This object method rolls back the current database transaction and starts a new one.  This means that all database changes (via DataObject persist and delete methods) since the last commit or rollback (or DataStore object creation if no commits or rollbacks have been done) are undone. 

This method takes no parameters and returns void.

### reset
This object method resets the database to the state it was in after a connection with reset.  This can be used in testing.

This method takes no parameters and returns void.

### createType
This object method is used to create new types in the system.  
On connection to a database with reset or after after reset is called, there will be type objects defined in the system: Type and Property.  Types have one or more properties and these are represented in the system by Type and Property objects.

While you can create Types by using createDataObject, it is recommended that this convenience method be used.

This method takes the following parameter:
* Type name - String - the name of the new type - this must not be a previously defined Type or "Type" or "Property"
This method returns a DataObject that represents the new Type.

### createProperty
This object method is used to create new properties for types.  Properties represent the different named values that can be stored for objects of a particular type.  Currently all properties are optional.  New properties can be dynamically added to a type.

While you can create Properties by using createDataObject, it is recommended that this convenience method be used. 

This method takes the following parameters:
* typeObject - DataObject - a DataObject representing the Type - usually obtained from createType or fetchDataObject
* propertyName - String - the name of the property - this must be unique within the type - different types can have properties with the same name
* dataType - String - this is "String" for string property types or the name of a previously defined  Type
This method returns a DataObject for the property.

### getTypeNames
This object method is used to get a list of all the property names defined in the system.

This method takes no parameters.

This method returns a List of Java String objects for the types that have been defined.

### getPropertiesForType
This object method is used to information about the properties for a type.

This method takes the following parameter:
* typeName - String - the name of the type
This method returns a List of PropertyInfo objects.  Each PropertyInfo object represents a property.  The PropertyInfo object has two methods:
* getName - returns the name of the property
* getDataType - returns the name of the data type for the property - either "String" or the name of a type

### createDataObject
This object method is used to create new objects for a type.

This method takes the following parameter:
* typeName - String - the name of a previously defined type
This method returns a new DataObject of that type.  Note that the object ID will not be set until presist is called on the DataObject.

### fetchDataObjects
This object method is used to fetch one or more objects from the database.

This method takes a key object which is used to determine which data objects to fetch.  The key data object is created using createDataObject and one or more properties for that object can be set.  At this time, if the property is another data object, then that data object must be first fetched from the database using fetchDataObjects with its own key before being used as a property value for a key.

This method will return all data objects that have the same property values as those property values set in the key.  If a property value is not set, then it is not used in fetching data objects from the database.

This method takes the following property:
* key - DataObject - a DataObject with the values set that must match the returned data objects
This method returns a List of DataObject objects.  This list can be empty if not data objects in the database match the property values set in the key.

### fetchDataObjectById
This object method is used to fetch a single data object by its object ID.

This method takes the following parameter:
* objectId - long - the object ID for a data object
This method returns the DataObject that matches the key or null if there is no such object with that object ID.

## DataObject

DataObject objects are used to create data objects, get them from the database, get and set properties for them, perist them (store or update), and delete them from the database.

### getString
This object method gets String type properties from data objects.

This method takes the following parameter:
* propertyName - String - the name of the property
This method returns the String value of the property or null if there is no value for that property.

If this method is called for a property that has a data type of other than String or for an unknown property,  a DBException is raised.

### getObject
This object method gets property values that are not Strings from data objects.

This method takes the following parameter:
* propertyName - String - the name of the property
This method returns the DataObject for the property or null if there is no value for that property.

If this method is called for a property that has a data type of String or for an unknown property,  a DBException is raised.

### getObjectId
This object method gets object IDs for property values that are not Strings from data objects.

This method can be used to determine whether or not a property that is a Type has a value without causing the sytem to actully fetch that property.  If later the actual object is wanted, then fetchDataObjectById can be called on the DataStore object.

This method takes the following parameter:
* propertyName - String - the name of the property
This method returns a Long object for the object ID for the property or null if there is no value for that property.

If this method is called for a property that has a data type of String or for an unknown property,  a DBException is raised.

### setString
This object method sets a String value for a property.

This method takes the following parameters:
* propertyName - String - the name of the property
* value - String - the value for that property
This method returns void.

If this method is called for a property that has a data type other than String or for an unknown property, a DBException is raised.

### setObject
This object method sets a DataObject value for a property.

This method takes the following parameters:
* propertyName - String - the name of the property
* value - DataObject - the value for that property
This method returns void.

If this method is called for a property that has a data type of String or for an unknown property, a DBException is raised.

### persist
This object method persists a data object.  If this object was created using the DataStore createObject  method then a new object is stored in the database.  This store operation will assign an object ID to the object.  If this object was read from the data store, then the object will be updated (currently deleted then stored in the current implementation).

This method takes no parameters and returns void.

### getId
This object method returns the object ID for an object.  This will return zero on a new object that has not been persisted in the database.

### getType
This object method returns a DataObject for the type of an object.
