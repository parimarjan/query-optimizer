import java.util.*;
import java.sql.*;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.cli.*;

class Main {

  private static Option newOption(String option, String helper) {
    Option opt = new Option(option, true, helper);
    opt.setRequired(false);
    return opt;
  }

  private static CommandLine parseArgs(String[] args) {
    Options options = new Options();
    options.addOption(newOption("port", "port number for zmq server"));
    options.addOption(newOption("query", "query number to run"));
    options.addOption(newOption("mode", "test or train"));
    options.addOption(newOption("onlyFinalReward", "reward at every step, or only at end. Boolean: 0 or 1"));
    options.addOption(newOption("lopt", "Use the LoptOptimizeJoinRule planner or not. boolean: 0 or 1"));
    options.addOption(newOption("python", "Use the planner to support the python controlled open-ai style environment or not. boolean: 0 or 1"));
    options.addOption(newOption("exhaustive", "use exhaustive search planner or not"));

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

  private static void updateQueries(int nextQuery, ArrayList<Integer> queries) {
    System.out.println("***************************");
    System.out.println("running query " + nextQuery);
    System.out.println("***************************");
    if (nextQuery == -1) {
      // even version
      for (int i = 0; i < 41; i++) {
        if (i % 2 == 0) {
          queries.add(i);
        }
      }
    } else if (nextQuery == -2) {
        // odd version
        for (int i = 0; i < 41; i++) {
          if (i % 2 != 0) {
            queries.add(i);
          }
        }
    } else {
      queries.add(nextQuery);
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArgs(args);
    Integer nextQuery = Integer.parseInt(cmd.getOptionValue("query", "0"));
    Integer port = Integer.parseInt(cmd.getOptionValue("port", "5555"));
    String mode = cmd.getOptionValue("mode", "train");
    boolean onlyFinalReward = (Integer.parseInt(cmd.getOptionValue("onlyFinalReward", "0")) == 1);
    boolean lopt = (Integer.parseInt(cmd.getOptionValue("lopt", "1")) == 1);
    boolean python = (Integer.parseInt(cmd.getOptionValue("python", "1")) == 1);
    boolean exhaustive = (Integer.parseInt(cmd.getOptionValue("exhaustive", "0")) == 1);

    // FIXME: helper utility to just print out all the options?
    System.out.println("using zmq port " + port);
    System.out.println("mode " + mode);
    System.out.println("onlyFinalReward " + onlyFinalReward);
    System.out.println("boolean onlyFinalReward " + (onlyFinalReward));
    System.out.println("LOpt " + lopt);
    System.out.println("python " + python);
    System.out.println("exhaustive " + exhaustive);

    ArrayList<QueryOptExperiment.PLANNER_TYPE> plannerTypes = new ArrayList<QueryOptExperiment.PLANNER_TYPE>();
    if (exhaustive) {
      plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.EXHAUSTIVE);
    }
    if (lopt) {
      plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.LOpt);
    }
    if (python) {
      plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.RL);
    }

    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.RANDOM);
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.DEBUG);
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.BUSHY);
    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.ORIG_JOIN_ORDER);

    QueryOptExperiment exp = null;
    try {
        exp = new QueryOptExperiment("jdbc:calcite:model=pg-schema.json", plannerTypes, QueryOptExperiment.QUERIES_DATASET.JOB, port, onlyFinalReward);
    } catch (Exception e) {
        System.err.println("Sql Exception!");
        throw e;
    }
    ArrayList<Integer> queries = new ArrayList<Integer>();
    updateQueries(nextQuery, queries);
    QueryOptExperiment.getZMQServer().curQuerySet = queries;
    if (mode.equals("train")) {
      exp.train(queries);
    } else if (mode.equals("test")) {
      exp.test(queries);
    } else if (mode.equals("run")) {
      // FIXME: test functionality for this
      //exp.run(exp.allSqlQueries);
    }
  }
}

