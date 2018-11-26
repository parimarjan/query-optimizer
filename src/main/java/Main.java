import java.util.*;
import java.sql.*;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.cli.*;

class Main {

  public static CommandLine parseArgs(String[] args) {
      Options options = new Options();
      Option port = new Option("p", "port", true, "port number for zmq server");
      port.setRequired(false);
      options.addOption(port);

      Option output = new Option("q", "query", true, "query num to run");
      output.setRequired(false);
      options.addOption(output);

      Option mode = new Option("m", "mode", true, "test, or train");
      mode.setRequired(false);
      options.addOption(mode);

      CommandLineParser parser = new DefaultParser();
      HelpFormatter formatter = new HelpFormatter();
      CommandLine cmd = null;

      try {
          cmd = parser.parse(options, args);
      } catch (ParseException e) {
          System.out.println(e.getMessage());
          formatter.printHelp("utility-name", options);
          System.exit(1);
      }

      return cmd;
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArgs(args);
    // FIXME: generalize nextQuery
    Integer nextQuery = Integer.parseInt(cmd.getOptionValue("query"));
    Integer port = Integer.parseInt(cmd.getOptionValue("port"));
    String mode = cmd.getOptionValue("mode", "train");
    System.out.println("using zmq port " + port);
    System.out.println("mode " + mode);

    ArrayList<QueryOptExperiment.PLANNER_TYPE> plannerTypes = new ArrayList<QueryOptExperiment.PLANNER_TYPE>();
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
        exp = new QueryOptExperiment("jdbc:calcite:model=pg-schema.json", plannerTypes, QueryOptExperiment.QUERIES_DATASET.JOB, port);
    } catch (Exception e) {
        System.err.println("Sql Exception!");
        throw e;
    }
    //exp.run(exp.allSqlQueries);
    ArrayList<Integer> queries = new ArrayList<Integer>();
    //int nextQuery = ThreadLocalRandom.current().nextInt(0, exp.allSqlQueries.size());
    System.out.println("***************************");
    System.out.println("running query " + nextQuery);
    System.out.println("***************************");
    if (nextQuery == -1) {
      for (int i = 0; i < 41; i++) {
        //nextQuery = ThreadLocalRandom.current().nextInt(0, exp.allSqlQueries.size());
        if (i % 2 == 0) {
          nextQuery = i;
          queries.add(nextQuery);
        }
      }
    } else if (nextQuery == -2) {
      // odd version
      for (int i = 0; i < 41; i++) {
        if (i % 2 != 0) {
          nextQuery = i;
          queries.add(nextQuery);
        }
      }

    } else {
      queries.add(nextQuery);
    }
    QueryOptExperiment.getZMQServer().curQuerySet = queries;
    if (mode.equals("train")) {
      exp.train(queries);
    } else {
      exp.test(queries);
    }
  }
}

