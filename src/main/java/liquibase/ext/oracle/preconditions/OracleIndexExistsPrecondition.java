package liquibase.ext.oracle.preconditions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.PreconditionErrorException;
import liquibase.exception.PreconditionFailedException;
import liquibase.exception.ValidationErrors;
import liquibase.exception.Warnings;
import liquibase.precondition.Precondition;

public class OracleIndexExistsPrecondition extends OraclePrecondition {

	private String indexName;
	private String tableName;
	private String columnNames;

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName( String indexName ) {
		this.indexName = indexName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName( String tableName ) {
		this.tableName = tableName;
	}

	public String getColumnNames() {
		return columnNames;
	}

	public void setColumnNames( String columnNames ) {
		this.columnNames = columnNames;
	}

	public String getName() {
		return "oracleIndexExists";
	}

	public Warnings warn( Database database ) {
		return new Warnings();
	}

	public ValidationErrors validate( Database database ) {
		ValidationErrors validationErrors = new ValidationErrors();
		if ( getIndexName() == null && getTableName() == null && getColumnNames() == null ) {
			validationErrors.addError( "indexName OR tableName and columnNames is required" );
		}
		return validationErrors;
	}

	public void check( Database database, DatabaseChangeLog changeLog, ChangeSet changeSet ) throws PreconditionFailedException, PreconditionErrorException {
		JdbcConnection connection = (JdbcConnection) database.getConnection();

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			if ( getIndexName() != null ) {
				final String sql = "select count(*) from all_indexes where upper(index_name) = upper(?) and upper(owner) = upper(?)";
				ps = connection.prepareStatement( sql );
				ps.setString( 1, getIndexName() );
				ps.setString( 2, database.getLiquibaseSchemaName() );
				rs = ps.executeQuery();
				if ( !rs.next() || rs.getInt( 1 ) <= 0 ) {
					throw new PreconditionFailedException( String.format( "The index '%s.%s' was not found.", database.getLiquibaseSchemaName(), getIndexName() ), changeLog, this );
				}
			} else {
				final String sql = "select index_name, column_name from all_ind_columns where upper(table_name) = upper (?) and upper(index_owner) = upper(?)";
				ps = connection.prepareStatement( sql );
				ps.setString( 1, getTableName() );
				ps.setString( 2, database.getLiquibaseSchemaName() );
				rs = ps.executeQuery();

				Map<String, List<String>> columnsMap = new HashMap<String, List<String>>();
				while ( rs.next() ) {
					String indexName = rs.getString( 1 );
					String columnName = rs.getString( 2 );
					List<String> cols = columnsMap.get( indexName );
					if ( cols == null ) {
						cols = new ArrayList<String>();
						columnsMap.put( indexName, cols );
					}
					cols.add( columnName.toUpperCase() );
				}

				String[] expectedColumns = getColumnNames().toUpperCase().split( "\\s*,\\s*" );

				// check for an index with all columns listed.
				for ( String index : columnsMap.keySet() ) {
					List<String> columnNames = columnsMap.get( index );
					if ( columnNames.size() == expectedColumns.length ) {
						if ( columnNames.containsAll( Arrays.asList( expectedColumns ) ) ) {
							return;
						}
					}
				}
				throw new PreconditionFailedException( String.format( "No index was found on table '%s.%s' with columns '%s'.", database.getLiquibaseSchemaName(), getTableName(), getColumnNames() ), changeLog, this );
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