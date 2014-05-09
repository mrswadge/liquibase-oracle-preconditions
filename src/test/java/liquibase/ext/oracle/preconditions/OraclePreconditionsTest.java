package liquibase.ext.oracle.preconditions;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.Contexts;
import liquibase.change.Change;
import liquibase.change.ChangeFactory;
import liquibase.change.ChangeMetaData;
import liquibase.changelog.ChangeLogHistoryService;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.exception.RollbackFailedException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.ext.oracle.preconditions.core.BaseTestCase;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OraclePreconditionsTest extends BaseTestCase {

	@Before
	public void setUp() throws Exception {
		changeLogFile = "liquibase/ext/oracle/preconditions/changelog.test.xml";
		connectToDB();
		cleanDB();
		liquiBase.update( (String) null );
	}

	@Test
	public void test() throws Exception {
		final List<String> expected = Arrays.asList( 
				"iftableexists", "iftablenotexists", "ifviewexists", "ifviewnotexists", 
				"ifindexexists1", "ifindexexists2", "ifindexnotexists1", "ifpkexists",
				"ifpknotexists"
		);
		Executor executor = ExecutorService.getInstance().getExecutor( liquiBase.getDatabase() );
		List<String> successes = executor.queryForList( new RawSqlStatement( "select * from testresults" ), String.class );
		assertTrue( successes.containsAll( expected ) );
	}

}
