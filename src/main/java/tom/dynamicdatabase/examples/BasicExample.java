package tom.dynamicdatabase.examples;

import java.util.List;

import tom.dynamicdatabase.DBException;
import tom.dynamicdatabase.DataObject;
import tom.dynamicdatabase.DataStore;
import tom.dynamicdatabase.TypeSystem;

/**
 * This is a basic example of how to use the dynamic database
 * <p>
 * This example:
 * <ul>
 * <li>Creates Address and Person types
 * <li>Creates a home and work address
 * <li>Creates a person, Tom, with the home and work addresses
 * <li>Creates a person, Joan, with no addresses
 * <li>Fetches all the person objects and displays them
 * </ul>
 * @author Tom Wimberg
 *
 */
public class BasicExample {

	// Connect information
	private static final String DB_DRIVER_CLASS_NAME = "org.h2.Driver";
	private static final String DB_URL = "jdbc:h2:./example";
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

	
	public static void main(String[] args) {
		
		// Create the address and person types
		try (DataStore ds = DataStore.open(DB_DRIVER_CLASS_NAME, DB_URL, DB_USER_NAME, DB_PASSWORD, true)) {
			
			// Create the types
			createTypes(ds);
			
			// Create a couple of addresses
			DataObject tomHomeAddress = createAddress(ds, "100 Pine Street", "Westford", "MA", "01886");
			DataObject tomWorkAddress = createAddress(ds, "22 Main Street", "Nashua", "NH", "03060");
			
			// Create a coupld of persons - one with addresses, one without
			createPerson(ds, "Tom", "Wimberg", tomHomeAddress, tomWorkAddress);
			createPerson(ds, "Joan", "Smith", null, null);
			
			// Commit the database
			ds.commit();
			
			// Now get all the persons - create a key without values to get all person objects
			DataObject personKey = ds.createDataObject(PERSON_TYPE_NAME);
			List<DataObject> personObjectList = ds.fetchDataObjects(personKey);
			
			// Display the persons that we created
			System.out.println("Persons:");
			for (DataObject personObject : personObjectList) {
				displayPerson(ds, personObject);
			}
			
		} catch (DBException e) {
			System.err.println("Exception caught: " + e.toString());
			e.printStackTrace(System.err);
		}		
	}
	
	/*
	 * Create the address and person types
	 */
	private static void createTypes(DataStore ds) throws DBException {

		DataObject addressType = ds.createType(ADDRESS_TYPE_NAME);
		ds.createProperty(addressType, ADDRESS_STREET,  TypeSystem.DATATYPE_STRING);
		ds.createProperty(addressType, ADDRESS_CITY, TypeSystem.DATATYPE_STRING);
		ds.createProperty(addressType, ADDRESS_STATE, TypeSystem.DATATYPE_STRING);
		ds.createProperty(addressType, ADDRESS_POSTAL_CODE, TypeSystem.DATATYPE_STRING);

		DataObject personType = ds.createType(PERSON_TYPE_NAME);
		ds.createProperty(personType, PERSON_FIRST_NAME, TypeSystem.DATATYPE_STRING);
		ds.createProperty(personType, PERSON_LAST_NAME, TypeSystem.DATATYPE_STRING);
		ds.createProperty(personType, PERSON_HOME_ADDRESS, ADDRESS_TYPE_NAME);
		ds.createProperty(personType, PERSON_WORK_ADDRESS, ADDRESS_TYPE_NAME);

	}
	
	/*
	 * Create an address
	 */
	private static DataObject createAddress(DataStore ds, String street, 
				String city, String state, String postalCode) throws DBException {
		DataObject address = ds.createDataObject(ADDRESS_TYPE_NAME);
		address.setString(ADDRESS_STREET, street);
		address.setString(ADDRESS_CITY, city);
		address.setString(ADDRESS_STATE, state);
		address.setString(ADDRESS_POSTAL_CODE, postalCode);
		address.persist();
		
		return address;
	}
	
	/*
	 * Create a person
	 */
	private static DataObject createPerson(DataStore ds, String firstName, String lastName, 
				DataObject homeAddress, DataObject workAddress) throws DBException {
	 	DataObject person = ds.createDataObject(PERSON_TYPE_NAME);
	 	person.setString(PERSON_FIRST_NAME, firstName);
	 	person.setString(PERSON_LAST_NAME, lastName);
	 	person.setObject(PERSON_HOME_ADDRESS, homeAddress);
	 	person.setObject(PERSON_WORK_ADDRESS, workAddress);
	 	person.persist();
	 	
	 	return person;
	}
	
	/*
	 * Display a person
	 */
	private static void displayPerson(DataStore ds, DataObject personObject) throws DBException {
		System.out.printf("First name: %s, last name: %s\n", 
				getNotNull(personObject.getString(PERSON_FIRST_NAME)),
				getNotNull(personObject.getString(PERSON_LAST_NAME)));
		
		DataObject homeAddress = personObject.getObject(PERSON_HOME_ADDRESS);
		if (homeAddress == null) {
			System.out.printf("  No home address\n");
		} else {
			System.out.printf("  Home address: %s, %s, %s %s\n",
					getNotNull(homeAddress.getString(ADDRESS_STREET)),
					getNotNull(homeAddress.getString(ADDRESS_CITY)),
					getNotNull(homeAddress.getString(ADDRESS_STATE)),
					getNotNull(homeAddress.getString(ADDRESS_POSTAL_CODE)));
		}
		
		DataObject workAddress = personObject.getObject(PERSON_WORK_ADDRESS);
		if (workAddress == null) {
			System.out.printf("  No work address\n");
		} else {
			System.out.printf("  Work address; %s, %s, %s %s\n",
					getNotNull(workAddress.getString(ADDRESS_STREET)),
					getNotNull(workAddress.getString(ADDRESS_CITY)),
					getNotNull(workAddress.getString(ADDRESS_STATE)),
					getNotNull(workAddress.getString(ADDRESS_POSTAL_CODE)));
		}
	}
	
	private static String getNotNull(String string) {
		return string == null ? "" : string;
	}
}
