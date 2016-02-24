package tom.dynamicdatabase.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import tom.dynamicdatabase.DBException;
import tom.dynamicdatabase.DataObject;
import tom.dynamicdatabase.Database;
import tom.dynamicdatabase.PropertyInfo;
import tom.dynamicdatabase.TypeSystem;

/**
 * Test cases - note that these cannot run in parallel since they all share the same database.
 * However, each test re-initializes the database so that they are independent.
 * @author Tom Wimberg
 *
 */
public class TestDatabase {
	
	// Connect information
	private static final String DB_DRIVER_CLASS_NAME = "org.h2.Driver";
	private static final String DB_URL = "jdbc:h2:./test";
	private static final String DB_USER_NAME = "sa";
	private static final String DB_PASSWORD = "";
	
	// Data type information
	private static final String ADDRESS_TYPE_NAME = "Address";
	private static final String ADDRESS_STREET = "Street";
	private static final String ADDRESS_CITY = "City";
	private static final String ADDRESS_STATE = "State";
	private static final String ADDRESS_POSTAL_CODE = "PostalCode";
	
	private static final String PERSON_TYPE_NAME = "Person";
	private static final String PERSON_FIRST_NAME = "FirstName";
	private static final String PERSON_LAST_NAME = "LastName";
	private static final String PERSON_HOME_ADDRESS = "HomeAddress";
	private static final String PERSON_WORK_ADDRESS = "WorkAddress";
	
	// Data for addresses
	private static final String TOM_HOME_STREET = "4 Depot Street";
	private static final String TOM_HOME_CITY = "Westford";
	private static final String TOM_HOME_STATE = "MA";
	private static final String TOM_HOME_POSTAL_CODE = "01886";
	
	private static final String TOM_WORK_STREET = "477 Main";
	private static final String TOM_WORK_CITY = "Nashua";
	private static final String TOM_WORK_STATE = "NH";
	private static final String TOM_WORK_POSTAL_CODE = "03060";
	
	// Data for people
	private static final String TOM_FIRST_NAME = "Tom";
	private static final String TOM_LAST_NAME = "Wimberg";
	private static final String TOM2_FIRST_NAME = "Tom2";
	
	private static final String JOAN_FIRST_NAME = "Joan";
	private static final String JOAN_LAST_NAME = "Smith";
	
	private static final String PRINCE_LAST_NAME = "Prince";

	/*
	 * Error cases - Database methods 
	 */
	
	@Test
	public void testOpenBadClass() {
		try (Database db = Database.connect("tom.nosuchclass", DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {
			fail("Bad class did not cause exception");
		} catch (DBException e) {
			// This is expected
		}
	}
	
	@Test
	public void testOpenBadJDBCConnect() {
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, "baduser", "", true)) {
			fail("Bad username did not cause exception");
		} catch (DBException e) {
			// This is expected
		}
	}
	
	@Test
	public void testReset() {
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {
			db.reset();
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());
		}
	}
	
	@Test
	public void testCreateDataObjectNoSuchType() {
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {
			try {
				@SuppressWarnings("unused")
				DataObject newObject = db.createDataObject(PERSON_TYPE_NAME);
				fail("Unknown type did not cause exception");
			} catch (DBException e) {
				// This is expected
			}
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());	
		}
	}
		
	@Test
	public void testSetStringBadPropertyName() {
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {
			createAddressType(db);
			createPersonType(db);
			DataObject person = db.createDataObject(PERSON_TYPE_NAME);
			try {
				person.setString("bad", "test");
				fail("Unknown property name did not cause exception");
			} catch (DBException e) {
				// This is expected
			}
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());
		}
	}
	
	@Test
	public void testSetStringBadPropertyDataType() {
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {
			createAddressType(db);
			createPersonType(db);
			DataObject person = db.createDataObject(PERSON_TYPE_NAME);
			try {
				person.setString(PERSON_HOME_ADDRESS, "test");
				fail("Unknown property name did not cause exception");
			} catch (DBException e) {
				// This is expected
			}
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());
		}
	}

	/*
	 * Success test - this contains all the successful tests of functionality
	 */
	@Test
	public void testSuccessful() {
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {

			// Create the Address data type
			createAddressType(db);
			
			// Create the person type
			createPersonType(db);
			
			// Commit what we have so far
			db.commit();
			
			// At this point we should have 4 data types
			List<String> typeNameList = db.getTypeNames();
			assertTrue("Wrong number of data types", typeNameList.size() == 4);
			
			// At this point we should have 4 properties for Address 
			List<PropertyInfo> propertyInfoList = db.getPropertiesForType(ADDRESS_TYPE_NAME);
			assertTrue("Wrong number of properties for Address", propertyInfoList.size() == 4);
			
			// Make sure we find no people at this point
			DataObject noPersonKey = db.createDataObject(PERSON_TYPE_NAME);
			List<DataObject> noPeopleObjectList = db.fetchDataObjects(noPersonKey);
			assertTrue("People retrieved when there should be none", noPeopleObjectList.size() == 0);
			
			// Create some addresses
			DataObject tomHomeAddress = createAddress(db, TOM_HOME_STREET, TOM_HOME_CITY, 
					TOM_HOME_STATE, TOM_HOME_POSTAL_CODE);
			DataObject tomWorkAddress = createAddress(db, TOM_WORK_STREET, TOM_WORK_CITY,
					TOM_WORK_STATE, TOM_WORK_POSTAL_CODE);
			
			// Create a person with everything
			DataObject tomObject = createPerson(db, TOM_FIRST_NAME, TOM_LAST_NAME,
					tomHomeAddress, tomWorkAddress);
			long tomObjectId = tomObject.getId();
			
			// Create a person with null objects
			createPerson(db, JOAN_FIRST_NAME, JOAN_LAST_NAME, null, null);
			// Create a person with null name and objects
			createPerson(db, null, PRINCE_LAST_NAME, null, null);
			
			
			
			// Commit what we have so far
			db.commit();
			
			// Test fetching stuff
			
			// There should now be two address objects and three person objects
			DataObject addressKey = db.createDataObject(ADDRESS_TYPE_NAME);
			List<DataObject> addressObjectList = db.fetchDataObjects(addressKey);
			assertTrue("Wrong number of address objects found", addressObjectList.size() == 2);
			
			DataObject personKey = db.createDataObject(PERSON_TYPE_NAME);
			List<DataObject> personObjectList = db.fetchDataObjects(personKey);
			assertTrue("Wrong number of person objects found", personObjectList.size() == 3);
			
			// Find the Tom person object - test getting objects from the database
			DataObject tomKey = db.createDataObject(PERSON_TYPE_NAME);
			tomKey.setString(PERSON_LAST_NAME, TOM_LAST_NAME);
			List<DataObject> tomObjectListDb = db.fetchDataObjects(tomKey);
			assertTrue("Wrong number of Tom person objects found", tomObjectListDb.size() == 1);
			DataObject tomObjectDb = tomObjectListDb.get(0);
			assertTrue("Tom first name is wrong", tomObjectDb.getString(PERSON_FIRST_NAME).equals(TOM_FIRST_NAME));
			DataObject tomHomeAddressDb = tomObjectDb.getObject(PERSON_HOME_ADDRESS);
			assertNotNull("Tom home address is null", tomHomeAddressDb);
			assertTrue("Tom home street is wrong", compareStr(tomHomeAddressDb.getString(ADDRESS_STREET), TOM_HOME_STREET));
			
			// Test the Tom object for non-data stuff
			assertTrue("Tom object ID not same", tomObjectId == tomObjectDb.getId());
			assertTrue("Tom object type name wrong", tomObjectDb.getTypeName() == PERSON_TYPE_NAME);
			
			// Make sure Joan has no home address
			DataObject joanKey = db.createDataObject(PERSON_TYPE_NAME);
			joanKey.setString(PERSON_LAST_NAME, JOAN_LAST_NAME);
			List<DataObject> joanObjectListDb = db.fetchDataObjects(joanKey);
			assertTrue("Wrong number of Joan person objects found", joanObjectListDb.size() == 1);
			DataObject joanHomeAddressDb = joanObjectListDb.get(0).getObject(PERSON_HOME_ADDRESS);
			assertNull("Joan has a home address", joanHomeAddressDb);
			
			// Make sure Prince has no first name
			DataObject princeKey = db.createDataObject(PERSON_TYPE_NAME);
			princeKey.setString(PERSON_LAST_NAME, PRINCE_LAST_NAME);
			List<DataObject> princeObjectListDb = db.fetchDataObjects(princeKey);
			assertTrue("Wrong number of Prince person objects found", princeObjectListDb.size() == 1);
			String princeFirstName = princeObjectListDb.get(0).getString(PERSON_FIRST_NAME);
			assertNull("Prince got a first name", princeFirstName);
			
			// Update Tom's first name
			tomObjectDb.setString(PERSON_FIRST_NAME, TOM2_FIRST_NAME);
			tomObjectDb.persist();
			
			// Now see if the update worked
			tomObjectListDb = db.fetchDataObjects(tomKey);
			assertTrue("Wrong number of Tom person objects found", tomObjectListDb.size() == 1);
			tomObjectDb = tomObjectListDb.get(0);
			assertTrue("Tom first name is wrong", tomObjectDb.getString(PERSON_FIRST_NAME).equals(TOM2_FIRST_NAME));
			
			// Delete Tom
			tomObjectDb.delete();
			
			// Make sure Tom is gone
			tomObjectListDb = db.fetchDataObjects(tomKey);
			assertTrue("Wrong number of Tom person objects found", tomObjectListDb.size() == 0);

			// Make sure there are only two person objects
			personObjectList = db.fetchDataObjects(personKey);
			assertTrue("Wrong number of person objects found", personObjectList.size() == 2);
			
			// Finally commit what we have
			db.commit();
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());
		}

		// Now try to see if we still have that data when we re-open the database
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, false)) {
			
			// There should now be two address objects and three person objects
			DataObject addressKey = db.createDataObject(ADDRESS_TYPE_NAME);
			List<DataObject> addressObjectList = db.fetchDataObjects(addressKey);
			assertTrue("Wrong number of address objects found", addressObjectList.size() == 2);

			// Get Joan
			DataObject joanKey = db.createDataObject(PERSON_TYPE_NAME);
			joanKey.setString(PERSON_LAST_NAME, JOAN_LAST_NAME);
			List<DataObject> joanObjectListDb = db.fetchDataObjects(joanKey);
			assertTrue("Wrong number of Joan person objects found", joanObjectListDb.size() == 1);

			// Delete Joan
			joanObjectListDb.get(0).delete();
			
			// Now rollback the transaction
			db.rollback();
			
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());
		}
		
		// Finally see if Joan is still there
		try (Database db = Database.connect(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, false)) {
			// Get Joan
			DataObject joanKey = db.createDataObject(PERSON_TYPE_NAME);
			joanKey.setString(PERSON_LAST_NAME, JOAN_LAST_NAME);
			List<DataObject> joanObjectListDb = db.fetchDataObjects(joanKey);
			assertTrue("Wrong number of Joan person objects found", joanObjectListDb.size() == 1);
			
		} catch (DBException e) {
			fail("Exception caught: " + e.toString());
		}
	}
	
	// Utility methods that are used in the above
	
	/*
	 * Create an address data type
	 */
	private void createAddressType(Database db) throws DBException {
		DataObject addressType = db.createType(ADDRESS_TYPE_NAME);
		db.createProperty(addressType, ADDRESS_STREET,  TypeSystem.DATATYPE_STRING);
		db.createProperty(addressType, ADDRESS_CITY, TypeSystem.DATATYPE_STRING);
		db.createProperty(addressType, ADDRESS_STATE, TypeSystem.DATATYPE_STRING);
		db.createProperty(addressType, ADDRESS_POSTAL_CODE, TypeSystem.DATATYPE_STRING);
	}
	
	/*
	 * Create a person data type with five properties - address type must be defined first
	 */
	private void createPersonType(Database db) throws DBException {
		DataObject personType = db.createType(PERSON_TYPE_NAME);
		db.createProperty(personType, PERSON_FIRST_NAME, TypeSystem.DATATYPE_STRING);
		db.createProperty(personType, PERSON_LAST_NAME, TypeSystem.DATATYPE_STRING);
		db.createProperty(personType, PERSON_HOME_ADDRESS, ADDRESS_TYPE_NAME);
		db.createProperty(personType, PERSON_WORK_ADDRESS, ADDRESS_TYPE_NAME);
	}
	
	/*
	 * Create an address object
	 */
	private DataObject createAddress(Database db, String street, 
			String city, String state, String postalCode) throws DBException {
		DataObject address = db.createDataObject(ADDRESS_TYPE_NAME);
		address.setString(ADDRESS_STREET, street);
		address.setString(ADDRESS_CITY, city);
		address.setString(ADDRESS_STATE, state);
		address.setString(ADDRESS_POSTAL_CODE, postalCode);
		
		// Check if we can get the data back
		assertTrue("Address street wrong", compareStr(address.getString(ADDRESS_STREET), street));
		assertTrue("City wrong", compareStr(address.getString(ADDRESS_CITY), city));
		assertTrue("State wrong", compareStr(address.getString(ADDRESS_STATE), state));
		assertTrue("Postal code wrong", compareStr(address.getString(ADDRESS_POSTAL_CODE), postalCode));
		
		// At this point object id should be zero
		assertTrue("Object id not zero", address.getId() == 0);
		
		// Now persist the object
		address.persist();
		
		// Now the object id should not be zero
		assertTrue("Object id zero", address.getId() != 0);
		
		return address;
	}
	
	/*
	 * Create a person object
	 */
	private DataObject createPerson(Database db, String firstName, String lastName, 
			DataObject homeAddress, DataObject workAddress) throws DBException {
	 	DataObject person = db.createDataObject(PERSON_TYPE_NAME);
	 	person.setString(PERSON_FIRST_NAME, firstName);
	 	person.setString(PERSON_LAST_NAME, lastName);
	 	person.setObject(PERSON_HOME_ADDRESS, homeAddress);
	 	person.setObject(PERSON_WORK_ADDRESS, workAddress);
	 	
	 	// Check if we can get the data back
	 	assertTrue("First name wrong", compareStr(person.getString(PERSON_FIRST_NAME), firstName));
	 	assertTrue("Last name wrong", compareStr(person.getString(PERSON_LAST_NAME), lastName));
	 	// Should get the exact same objects back for addresses at this point
	 	assertTrue("Home address wrong", person.getObject(PERSON_HOME_ADDRESS) == homeAddress);
	 	assertTrue("Work address wrong", person.getObject(PERSON_WORK_ADDRESS) == workAddress);
	 	
	 	// At this point the object id should be zero
	 	assertTrue("Object is not zero", person.getId() == 0);
	 	
	 	// Now persist the object
	 	person.persist();
	 	
	 	// The the object id should not be zero
	 	assertTrue("Object id zero", person.getId() != 0);
	 	
	 	return person;
	}
	
	/*
	 * Compare two strings - allow null to equal to null
	 */
	private boolean compareStr(String string1, String string2) {
		if (string1 == null && string2 == null) return true;
		if (string1 == null || string2 == null) return false;
		return string1.equals(string2);
	}
	
}
