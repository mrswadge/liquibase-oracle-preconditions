package liquibase.ext.oracle.preconditions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.precondition.Precondition;

public abstract class OraclePrecondition implements Precondition {

	void closeSilently( PreparedStatement ps ) {
		if ( ps != null ) {
			try {
				ps.close();
			} catch ( SQLException e ) {
			}
		}
	}

	void closeSilently( ResultSet rs ) {
		if ( rs != null ) {
			try {
				rs.close();
			} catch ( SQLException e ) {
			}
		}
	}

}
