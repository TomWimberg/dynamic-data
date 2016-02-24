package tom.dynamicdatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Initialize the database and insert the base data type objects
 * 
 * @author Tom Wimberg
 *
 */
class InitializeDatabase {
	
	private static final List<String> initScript = Arrays.asList(
			//
			// Delete any old stuff
			//
			"drop table if exists value_object",
			"drop table if exists value_string",
			"drop table if exists object",
			"drop sequence if exists data_sequence",
			"create sequence data_sequence start with 100",
			 //"create table business_entity id number(19,0) not null, path varchar2(255), name varchar2(255), primary key(id))",
			 //"create table user_entity",
			 // 
			 //  The tables for the dynamic business objects
			 // 
			 "create table object (id number(19,0) not null, metadata_id number(19,0) not null, primary key (id))",
			 "create table value_string(object_id number(19,0) not null, metadata_id number(19,0) not null, data varchar2(255) not null, primary key (object_id, metadata_id))",
			 "create table value_object(object_id number(19,0) not null, metadata_id number(19,0) not null, data number(19,0) not null, primary key (object_id, metadata_id))",
			 //
			 // Define the Type and Property objects - these are the base of the metadata
			 // This is needed in database for foreign key references
			 //
			 // Define 'Type' type object
			 // Metadata ID = 1 - a Type
			 "insert into object (id, metadata_id) values (1, 1)",
			 "insert into value_string (object_id, metadata_id, data) values (1, 5, 'Type')",
			 // Define 'Name' property object for type 'Type'
			 // Metadata  ID = 5 - a 'Name' property for a 'Type' type
			 "insert into object (id, metadata_id) values (5, 2)",
			 "insert into value_object (object_id, metadata_id, data) values (5, 6, 1)",
			 "insert into value_string (object_id, metadata_id, data) values (5, 7, 'Name')",
			 "insert into value_string (object_id, metadata_id, data) values (5, 8, 'String')",
			 //
			 // Define 'Property' type
			 // Metadata ID = 2 - a Property
			 "insert into object (id, metadata_id) values (2, 1)",
			 "insert into value_string (object_id, metadata_id, data) values (2, 5, 'Property')",
			 // Define 'PropertyTypeOwner' property object for type 'Property' 
			 // Metadata ID = 6 - a 'PropertyTypeOwner' property for a 'Property' type
			 "insert into object (id, metadata_id) values (6, 2)",
			 "insert into value_object (object_id, metadata_id, data) values (6, 6, 2)",
			 "insert into value_string (object_id, metadata_id, data) values (6, 7, 'Owner')",
			 "insert into value_string (object_id, metadata_id, data) values (6, 8, 'Type')",
			 // Define 'PropertyName' property object for type 'Property' 
			 // Metadata ID = 7 - a 'PropertyName' property for a 'Property' type
			 "insert into object (id, metadata_id) values (7, 2)",
			 "insert into value_object (object_id, metadata_id, data) values (7, 6, 2)",
			 "insert into value_string (object_id, metadata_id, data) values (7, 7, 'Name')",
			 "insert into value_string (object_id, metadata_id, data) values (7, 8, 'String')",
			 // Define 'PropertyDataType' property object for type 'Property' 
			 // Metadata ID = 8 - a 'PropertyDataType' property for a 'Property' type
			 "insert into object (id, metadata_id) values (8, 2)",
			 "insert into value_object (object_id, metadata_id, data) values (8, 6, 2)",
			 "insert into value_string (object_id, metadata_id, data) values (8, 7, 'Type')",
			 "insert into value_string (object_id, metadata_id, data) values (8, 8, 'String')",
			 //
			 // Define foreign keys for dynamic objects after type metadata is defined
			 // since some of the metadata definitions require circular definitions.
			 "alter table object add constraint object_metadata_fk foreign key(metadata_id) references object",
			 "alter table value_string add constraint value_string_object_fk foreign key (object_id) references object",
			 "alter table value_string add constraint value_string_metadata_fk foreign key (metadata_id) references object",
			 "alter table value_object add constraint value_object_object_fk foreign key (object_id) references object",
			 "alter table value_object add constraint value_object_metadata_fk foreign key (metadata_id) references object",
			 "alter table value_object add constraint value_object_object_data_fk foreign key (data) references object"
			 );
	
	/**
	 * Reset the database - delete and recreate tables and then insert the base
	 * meta data for type Type and Property - only for db
	 */
	static void reset(Connection con) throws DBException {
		
		try {
			
			for (String statement : initScript) {
				PreparedStatement ps = con.prepareStatement(statement);
				ps.execute();
				ps.close();
			}
			
			con.commit();
			
		} catch (SQLException e) {
			throw new DBException("Excdeption on db reset", e);
		}
		
	}

}
