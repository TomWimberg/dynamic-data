package tom.dynamicdatabase;

/**
 * This class is used to return information about properties
 * @author wimberg
 *
 */
public class PropertyInfo {
	
	private String name;
	private String dataType;
	
	/**
	 * Construct a property info object
	 * @param name		The name of the property
	 * @param dataType	The data type of the property
	 */
	public PropertyInfo(String name, String dataType) {
		this.name = name;
		this.dataType = dataType;
	}
	
	/**
	 * Get the name of the property
	 * @return		The name of the property
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Get the data type of the property
	 * @return		The data type of the property as a string
	 */
	public String getDataType() {
		return dataType;
	}

}
