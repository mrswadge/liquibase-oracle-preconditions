package liquibase.ext.oracle.preconditions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.changelog.visitor.ChangeExecListener;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;

public class OracleSeqeunceExistsPrecondition extends OraclePrecondition {

	private String sequenceName;

	public String getSequenceName() {
		return sequenceName;
	}

	public void setSequenceName( String sequenceName ) {
		this.sequenceName = sequenceName;
	}

	public String getName() {
		return "oracleSequenceExists";
	}

	public Warnings warn( Database database ) {
		return new Warnings();
	}

	public ValidationErrors validate( Database database ) {
		return new ValidationErrors();
	}

	public void check( Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener ) throws PreconditionFailedException, PreconditionErrorException {
		JdbcConnection connection = (JdbcConnection) database.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			final String sql = "select count(*) from all_sequences where upper(sequence_name) = upper(?) and upper(sequence_owner) = upper(?)";
			ps = connection.prepareStatement( sql );
			ps.setString( 1, getSequenceName() );
			ps.setString( 2, database.getLiquibaseSchemaName() );
			rs = ps.executeQuery();
			if ( !rs.next() || rs.getInt( 1 ) <= 0 ) {
				throw new PreconditionFailedException( String.format( "The sequence '%s.%s' was not found.", database.getLiquibaseSchemaName(), getSequenceName() ), changeLog, this );
			}
		} catch ( SQLException e ) {
			throw new PreconditionErrorException( e, changeLog, this );
		} catch ( DatabaseException e ) {
			throw new PreconditionErrorException( e, changeLog, this );
		} finally {
			closeSilently( rs );
			closeSilently( ps );
		}
	}

}