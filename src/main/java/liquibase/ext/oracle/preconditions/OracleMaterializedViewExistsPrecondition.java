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
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;

public class OracleMaterializedViewExistsPrecondition extends OraclePrecondition {

	private String viewName;

	public String getViewName() {
		return viewName;
	}

	public void setViewName( String viewName ) {
		this.viewName = viewName;
	}

	public String getName() {
		return "oracleMaterializedViewExists";
	}

	public Warnings warn( Database database ) {
		return new Warnings();
	}

	public ValidationErrors validate( Database database ) {
		return new ValidationErrors();
	}

	public void check( Database database, DatabaseChangeLog changeLog, ChangeSet changeSet, ChangeExecListener changeExecListener ) throws PreconditionFailedException, PreconditionErrorException {
		if ( ! check( database ) ) {
			throw new PreconditionFailedException( String.format( "The view '%s.%s' was not found.", database.getLiquibaseSchemaName(), getViewName() ), changeLog, this );
		}
	}
	
	public boolean check( Database database ) throws UnexpectedLiquibaseException {
		JdbcConnection connection = (JdbcConnection) database.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			final String sql = "select count(*) from all_mviews where upper(mview_name) = upper(?) and upper(owner) = upper(?)";
			ps = connection.prepareStatement( sql );
			ps.setString( 1, getViewName() );
			ps.setString( 2, database.getLiquibaseSchemaName() );
			rs = ps.executeQuery();
			if ( !rs.next() || rs.getInt( 1 ) <= 0 ) {
				return false;
			} else {
				return true;
			}
		} catch ( SQLException e ) {
			throw new UnexpectedLiquibaseException( "Failed on database view investigation.", e );
		} catch ( DatabaseException e ) {
			throw new UnexpectedLiquibaseException( "Failed on database view investigation.", e );
		} finally {
			closeSilently( rs );
			closeSilently( ps );
		}
	}

}