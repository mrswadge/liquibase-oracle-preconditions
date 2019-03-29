package liquibase.ext.oracle.preconditions;

import static org.junit.Assert.*;

import java.net.URL;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import liquibase.precondition.Precondition;
import liquibase.precondition.PreconditionFactory;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.ResourceAccessor;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.FileUtil;

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
				"ifpknotexists", "iffkexists", "iffknotexists", "ifsequenceexists",
				"ifsequencenotexists"
		);
		Executor executor = ExecutorService.getInstance().getExecutor( liquiBase.getDatabase() );
		List<String> successes = executor.queryForList( new RawSqlStatement( "select * from testresults" ), String.class );
		assertTrue( successes.containsAll( expected ) );
	}

	@Test
	public void testRegistry() throws Exception {
		Map<String, Class<? extends Precondition>> preconditions = PreconditionFactory.getInstance().getPreconditions();
		checkRegistry( preconditions, OracleCheckConstraintExistsPrecondition.class );
		checkRegistry( preconditions, OracleColumnExistsPrecondition.class );
		checkRegistry( preconditions, OracleForeignKeyExistsPrecondition.class );
		checkRegistry( preconditions, OracleIndexExistsPrecondition.class );
		checkRegistry( preconditions, OracleMaterializedViewExistsPrecondition.class );
		checkRegistry( preconditions, OraclePrimaryKeyExistsPrecondition.class );
		checkRegistry( preconditions, OracleSequenceExistsPrecondition.class );
		checkRegistry( preconditions, OracleTableExistsPrecondition.class );
		checkRegistry( preconditions, OracleUniqueConstraintExistsPrecondition.class );
		checkRegistry( preconditions, OracleViewExistsPrecondition.class );
	}

	private void checkRegistry( Map<String, Class<? extends Precondition>> registry, Class<? extends Precondition> clazz ) throws InstantiationException, IllegalAccessException {
		Precondition precondition = clazz.newInstance();
		String name = precondition.getName();
		Class<? extends Precondition> mappedClazz = registry.get( name );
		assertEquals( clazz, mappedClazz );
	}
	
}
