import java.util.*;
import java.sql.*;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.cli.*;

class Main {

  // FIXME: this should not be required.
  private static Option newOption(String option, String helper) {
    Option opt = new Option(option, true, helper);
    opt.setRequired(false);
    return opt;
  }

  private static CommandLine parseArgs(String[] args)
  {
    Options options = new Options();
    options.addOption(newOption("port", "port number for zmq server"));
    options.addOption(newOption("query", "query number to run"));
    options.addOption(newOption("onlyFinalReward", "reward at every step, or only at end. Boolean: 0 or 1"));
    options.addOption(newOption("lopt", "Use the LoptOptimizeJoinRule planner or not. boolean: 0 or 1"));
    options.addOption(newOption("python", "Use the planner to support the python controlled open-ai style environment or not. boolean: 0 or 1"));
    options.addOption(newOption("exhaustive", "use exhaustive search planner or not"));
    options.addOption(newOption("leftDeep", "use dynamic programming based left deep search planner or not"));
    options.addOption(newOption("verbose", "use exhaustive search planner or not"));
    options.addOption(newOption("train", ""));
    options.addOption(newOption("execOnDB", "base final reward on runtime"));
    options.addOption(newOption("costModel", "which cost model to use. '', 'CM1', 'CM2', 'CM3'"));
    options.addOption(newOption("dataset", "which dataset to use"));

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

  private static void updateQueries(int nextQuery, ArrayList<Integer> trainQueries,
      ArrayList<Integer> testQueries)
 {
    System.out.println("***************************");
    System.out.println("running query " + nextQuery);
    System.out.println("***************************");
    if (nextQuery == -1) {
      // even version
      for (int i = 0; i < 113; i++) {
        if (i % 2 == 0) {
          trainQueries.add(i);
        } else {
          testQueries.add(i);
        }
      }
    } else if (nextQuery == -2) {
        // odd version
        for (int i = 0; i < 113; i++) {
          if (i % 2 != 0) {
            trainQueries.add(i);
          } else {
            testQueries.add(i);
          }
        }
    } else if (nextQuery == -3) {
      for (int i = 0; i < 113; i++) {
        trainQueries.add(i);
        testQueries.add(i);
      }
    } else {
      trainQueries.add(nextQuery);
      testQueries.add(nextQuery);
    }
  }

  public static void main(String[] args) throws Exception
  {
    CommandLine cmd = parseArgs(args);
    Integer nextQuery = Integer.parseInt(cmd.getOptionValue("query", "0"));
    Integer port = Integer.parseInt(cmd.getOptionValue("port", "5555"));

    /// FIXME: handle booleans correctly
    boolean onlyFinalReward = (Integer.parseInt(cmd.getOptionValue("onlyFinalReward", "0")) == 1);
    boolean lopt = (Integer.parseInt(cmd.getOptionValue("lopt", "1")) == 1);
    boolean python = (Integer.parseInt(cmd.getOptionValue("python", "1")) == 1);
    boolean exhaustive = (Integer.parseInt(cmd.getOptionValue("exhaustive", "0")) == 1);
    boolean leftDeep = (Integer.parseInt(cmd.getOptionValue("leftDeep", "0")) == 1);
    boolean train = (Integer.parseInt(cmd.getOptionValue("train", "1")) == 1);

    boolean verbose = (Integer.parseInt(cmd.getOptionValue("verbose", "0")) == 1);

    String costModel = cmd.getOptionValue("costModel", "");
    //boolean execOnDB = cmd.hasOption("execOnDB");
    boolean execOnDB = (Integer.parseInt(cmd.getOptionValue("execOnDB", "0")) == 1);

    String dataset = cmd.getOptionValue("dataset", "JOB");

    // FIXME: helper utility to just print out all the options?
    System.out.println("using zmq port " + port);
    System.out.println("onlyFinalReward " + (onlyFinalReward));
    System.out.println("LOpt " + lopt);
    System.out.println("python " + python);
    System.out.println("exhaustive " + exhaustive);
    System.out.println("left deep search " + leftDeep);
    System.out.println("costModel " + costModel);
    System.out.println("dataset " + dataset);
    System.out.println("train " + train);
    System.out.println("execOnDB " + execOnDB);

    // FIXME: this should be part of the params interface as well.
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
    if (leftDeep) {
      plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.LEFT_DEEP);
    }

    //plannerTypes.add(QueryOptExperiment.PLANNER_TYPE.BUSHY);

    QueryOptExperiment exp = null;
    String dbUrl = "jdbc:calcite:model=pg-schema.json";

    QueryOptExperiment.Params params = new QueryOptExperiment.Params();
    params.execOnDB = execOnDB;
    params.dbUrl = dbUrl;
    params.python = python;

    try {
        exp = new QueryOptExperiment(dbUrl, plannerTypes, QueryOptExperiment.QUERIES_DATASET.getDataset(dataset), port, verbose, train, costModel, params);
    } catch (Exception e) {
        throw e;
    }

    ArrayList<Integer> trainQueries = new ArrayList<Integer>();
    ArrayList<Integer> testQueries = new ArrayList<Integer>();

    updateQueries(nextQuery, trainQueries, testQueries);

    QueryOptExperiment.getZMQServer().curQuerySet = trainQueries;
    exp.trainQueries = trainQueries;
    exp.testQueries = testQueries;
    exp.start();
  }
}

