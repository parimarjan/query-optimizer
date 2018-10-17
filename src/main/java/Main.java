import java.util.*;
import java.sql.*;
import java.sql.SQLException;

class Main {

    public static void usageAndExit() {
        System.out.println("");
        System.exit(1);
    }

    // FIXME: get rid of exception
    public static void main(String[] args) throws Exception {
		System.out.println("starting query processing");
        List<QueryOptExperiment.PLANNER_TYPE> plannerTypes = new ArrayList<QueryOptExperiment.PLANNER_TYPE>();
        // TODO: add command line flags for these
        //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.EXHAUSTIVE);
        plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.LOpt);
        QueryOptExperiment exp = null;
        try {
            exp = new QueryOptExperiment("jdbc:calcite:model=test-schema.json", plannerTypes, QueryOptExperiment.QUERIES_DATASET.JOB);
        } catch (Exception e) {
            System.err.println("Sql Exception!");
            System.exit(-1);
        }
        exp.run(exp.allSqlQueries);
    }
}

