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

public class OraclePrimaryKeyExistsPrecondition extends OraclePrecondition {

	private String primaryKeyName;
	private String tableName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName( String tableName ) {
		this.tableName = tableName;
	}

	public String getName() {
		return "oraclePrimaryKeyExists";
	}

	public String getPrimaryKeyName() {
		return primaryKeyName;
	}

	public void setPrimaryKeyName( String constraintName ) {
		this.primaryKeyName = constraintName;
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
			/*
			THE CONSTRAINT_TYPE will tell you what type of constraint it is
			
			R - Referential key ( foreign key)
			U - Unique key
			P - Primary key
			C - Check constraint
			 */
			final String sql = "select constraint_name from all_constraints where table_name = upper(?) and upper(owner) = upper(?) and constraint_type = 'P'";
			ps = connection.prepareStatement( sql );
			ps.setString( 1, getTableName() );
			ps.setString( 2, database.getLiquibaseSchemaName() );
			rs = ps.executeQuery();
			
			if ( !rs.next() ) {
				throw new PreconditionFailedException( String.format( "The primary key '%s' was not found on the table '%s.%s'.", getPrimaryKeyName(), database.getLiquibaseSchemaName(), getTableName() ), changeLog, this );
			} else {
				String name = rs.getString( 1 );
				if ( getPrimaryKeyName() != null && getPrimaryKeyName().length() > 0 ) {
					// check the name is the same, otherwise presume we are fine.
					if ( ! name.equalsIgnoreCase( getPrimaryKeyName() ) ) {
						throw new PreconditionFailedException( String.format( "The primary key '%s' was not found on the table '%s.%s'.", getPrimaryKeyName(), database.getLiquibaseSchemaName(), getTableName() ), changeLog, this );
					}
				}
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
    this.tableName = parsedNode.getChildValue(null, "tableName", String.class);
    this.primaryKeyName = parsedNode.getChildValue(null, "primaryKeyName", String.class);
	}
}