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
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

public class OracleViewExistsPrecondition extends OraclePrecondition {

	private String viewName;

	public String getViewName() {
		return viewName;
	}

	public void setViewName( String viewName ) {
		this.viewName = viewName;
	}

	public String getName() {
		return "oracleViewExists";
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
			final String sql = "select count(*) from all_views where upper(view_name) = upper(?) and upper(owner) = upper(?)";
			ps = connection.prepareStatement( sql );
			ps.setString( 1, getViewName() );
			ps.setString( 2, database.getLiquibaseSchemaName() );
			rs = ps.executeQuery();
			if ( !rs.next() || rs.getInt( 1 ) <= 0 ) {
				throw new PreconditionFailedException( String.format( "The view '%s.%s' was not found.", database.getLiquibaseSchemaName(), getViewName() ), changeLog, this );
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

	@Override
	public void load( ParsedNode parsedNode, ResourceAccessor resourceAccessor ) throws ParsedNodeException {
		super.load( parsedNode, resourceAccessor );
    this.viewName = parsedNode.getChildValue(null, "viewName", String.class);
	}
}