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
        //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.LOpt);
        //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.RANDOM);
        //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.DEBUG);
        //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.BUSHY);
        plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.RL);
        QueryOptExperiment exp = null;
        try {
            exp = new QueryOptExperiment("jdbc:calcite:model=pg-schema.json", plannerTypes, QueryOptExperiment.QUERIES_DATASET.SIMPLE);
        } catch (Exception e) {
            System.err.println("Sql Exception!");
            throw e;
        }
        exp.run(exp.allSqlQueries);
    }
}

