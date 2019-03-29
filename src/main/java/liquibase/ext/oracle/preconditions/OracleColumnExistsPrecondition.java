package liquibase.ext.oracle.preconditions;

import liquibase.precondition.core.ColumnExistsPrecondition;

public class OracleColumnExistsPrecondition extends ColumnExistsPrecondition {

	@Override
	public String getName() {
		return "oracleColumnExists";
	}
	
}
