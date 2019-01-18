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

public class OracleColumnExistsPrecondition extends OraclePrecondition {

	private String tableName;
	private String columnName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName( String tableName ) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName( String columnName ) {
		this.columnName = columnName;
	}

	public String getName() {
		return "oracleColumnExists";
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
			final String sql = "select count(*) from all_tab_columns where upper(table_name) = upper(?) and upper(column_name) = upper(?) and upper(owner) = upper(?)";
			ps = connection.prepareStatement( sql );
			ps.setString( 1, getTableName() );
			ps.setString( 2, getColumnName() );
			ps.setString( 3, database.getLiquibaseSchemaName() );
			rs = ps.executeQuery();
			if ( !rs.next() || rs.getInt( 1 ) <= 0 ) {
				throw new PreconditionFailedException( String.format( "The column '%s' was not found on table '%s.%s'.", getColumnName(), database.getLiquibaseSchemaName(), getTableName() ), changeLog, this );
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