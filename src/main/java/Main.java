import java.util.*;
import java.sql.*;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

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
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.RANDOM);
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.DEBUG);
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.BUSHY);
    plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.RL);
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.ORIG_JOIN_ORDER);
    QueryOptExperiment exp = null;
    try {
        exp = new QueryOptExperiment("jdbc:calcite:model=pg-schema.json", plannerTypes, QueryOptExperiment.QUERIES_DATASET.JOB);
    } catch (Exception e) {
        System.err.println("Sql Exception!");
        throw e;
    }
    //exp.run(exp.allSqlQueries);
    //exp.train(exp.allSqlQueries);
    ArrayList<String> trainingQueries = new ArrayList<String>();
    //int nextQuery = ThreadLocalRandom.current().nextInt(0, exp.allSqlQueries.size());
    //int nextQuery = 11;
    //System.out.println("***************************");
    //System.out.println("running query " + nextQuery);
    //System.out.println("***************************");
    //trainingQueries.add(exp.allSqlQueries.get(nextQuery));
    for (int i = 0; i < 10; i++) {
      trainingQueries.add(exp.allSqlQueries.get(i));
    }
    exp.train(trainingQueries);
  }
}

