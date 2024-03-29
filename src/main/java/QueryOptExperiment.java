import java.sql.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.tools.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.adapter.enumerable.*;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
// experimental:
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.ImmutableBitSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

//import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
//import org.apache.calcite.rel.RelToSqlConverter;
//import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Result;
import org.apache.calcite.sql.SqlNode;

// for parallel execution
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.calcite.rel.core.RelFactories;

// for loading / saving costs
import java.util.*;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;

/* Will contain all the parameters / data etc. to drive one end to end
 * experiment.
 */
public class QueryOptExperiment {

  private static CalciteConnection conn;
  private static String dbUrl;
  public String allOptCostsPersistentFN = "./allOptParCosts.ser";

  public enum PLANNER_TYPE
  {
    EXHAUSTIVE,
    LOpt,
    RANDOM,
    BUSHY,
    RL,
    LEFT_DEEP,
    GREEDY;

    // FIXME: not sure if we need to add other rules - like we
    // could add all of the Programs.RULE_SET here, and remove the
    // exhaustive rules above (that was done in heuristicJoinOrder)
    public static final ImmutableList<RelOptRule> EXHAUSTIVE_RULES =
        ImmutableList.of(ExhaustiveDPJoinOrderRule.INSTANCE,
                         FilterJoinRule.FILTER_ON_JOIN,
                         ProjectMergeRule.INSTANCE);
    public static final ImmutableList<RelOptRule> LEFT_DEEP_RULES =
        ImmutableList.of(LeftDeepJoinOrderRule.INSTANCE,
                         FilterJoinRule.FILTER_ON_JOIN,
                         ProjectMergeRule.INSTANCE);
    public static final ImmutableList<RelOptRule> LOPT_RULES =
        ImmutableList.of(MyLoptOptimizeJoinRule.INSTANCE,
                         FilterJoinRule.FILTER_ON_JOIN,
                         ProjectMergeRule.INSTANCE);
    // Note: need the second projection rule as otherwise the optimized
    // node from the joins was projecting all the fields before projecting
    // it down to only the selected fields
    public static final ImmutableList<RelOptRule> RL_RULES =
        ImmutableList.of(RLJoinOrderRule.INSTANCE,
                         FilterJoinRule.FILTER_ON_JOIN,
                         ProjectMergeRule.INSTANCE);

    public static final ImmutableList<RelOptRule> BUSHY_RULES =
        ImmutableList.of(MultiJoinOptimizeBushyRule.INSTANCE,
                        FilterJoinRule.FILTER_ON_JOIN,
                        ProjectMergeRule.INSTANCE);
    /// FIXME: implement these!!
    public static final ImmutableList<RelOptRule> RANDOM_RULES =
        ImmutableList.of(MultiJoinOptimizeBushyRule.INSTANCE);
    public static final ImmutableList<RelOptRule> GREEDY_RULES =
        ImmutableList.of(MultiJoinOptimizeBushyRule.INSTANCE);

    public List<RelOptRule> getRules() {
      ArrayList<RelOptRule> rules = new ArrayList<RelOptRule>();
      switch(this){
        case EXHAUSTIVE:
          RelOptRule exhRule = new ExhaustiveDPJoinOrderRule(RelFactories.LOGICAL_BUILDER);
          rules.add(exhRule);
          return rules;
        case LOpt:
          return LOPT_RULES;
        case RANDOM:
          return RANDOM_RULES;
        case BUSHY:
          return BUSHY_RULES;
        case RL:
          return RL_RULES;
        case LEFT_DEEP:
          return LEFT_DEEP_RULES;
        default:
          return null;
      }
    }

    public List<RelOptRule> getRules(String queryName) {
      ArrayList<RelOptRule> rules = new ArrayList<RelOptRule>();
      // TODO: add the common ones

        //ImmutableList.of(ExhaustiveDPJoinOrderRule.INSTANCE,
                         //FilterJoinRule.FILTER_ON_JOIN,
                         //ProjectMergeRule.INSTANCE);
      switch(this){
        case EXHAUSTIVE:
          ExhaustiveDPJoinOrderRule exhRule =
            new ExhaustiveDPJoinOrderRule(RelFactories.LOGICAL_BUILDER);
          exhRule.setQueryName(queryName);
          rules.add(exhRule);
          rules.add(FilterJoinRule.FILTER_ON_JOIN);
          rules.add(ProjectMergeRule.INSTANCE);
          return rules;

        // FIXME: initialize rules with sql etc. here
        case LOpt:
          MyLoptOptimizeJoinRule loptRule =
            new MyLoptOptimizeJoinRule(RelFactories.LOGICAL_BUILDER);
          loptRule.setQueryName(queryName);
          rules.add(loptRule);
          rules.add(FilterJoinRule.FILTER_ON_JOIN);
          rules.add(ProjectMergeRule.INSTANCE);
          return rules;
          //return LOPT_RULES;
        case RANDOM:
          return RANDOM_RULES;
        case BUSHY:
          return BUSHY_RULES;
        case RL:
          return RL_RULES;
        case LEFT_DEEP:
          return LEFT_DEEP_RULES;
        default:
          return null;
      }
    }
  }
  private ArrayList<PLANNER_TYPE> plannerTypes;
  /* actual volcanoPlanners generated using the above rules */
  private ArrayList<Planner> volcanoPlanners;

  /* keeps all the pluggable parameters for the experiment, mostly set through
   * the command line parsing in Main.java */
  public static class Params {

    // initalize these with their default values incase someone doesn't supply
    // them

    // If execOnDB is false, and onlyFinalReward is true, then we will
    // treat the final reward as a sum of all the intermediate rewards.
    //public boolean onlyFinalReward = false;
    public boolean execOnDB = false;
    public boolean verifyResults = false;
    public boolean recomputeFixedPlanners = true;
    public Integer maxExecutionTime = 1200;
    public boolean python = true;
    // pain, fucking pain
    public boolean getSqlToExecute = false;
    public String dbUrl = "";
    // FIXME: make this command line arg
    public String pgUrl = "jdbc:postgresql://localhost:5432/imdb";

    //public String user = "ubuntu";
    //public String pwd = "ubuntu";
    public String user = "pari";
    public String pwd = "";

    // clear cache after every execution
    public boolean clearCache = false;
    public String cardinalitiesModel = "file";
    public String cardinalitiesModelFile = "./pg.json";

    // num reps for runtimes
    public Integer numExecutionReps = 1;
    public boolean train = false;
    public String runtimeFileName = "allQueryRuntimes.json";
    // cardinalities dictionary.
    public HashMap<String, HashMap<String, Long>> cardinalities = null;
    public boolean useIndexNestedLJ;
    public Double scanCostFactor;
    public boolean testCardinalities;

    public Params() {

    }
  }

  private static Params params;

  // FIXME: move all these to Params
  public static ZeroMQServer zmq;

  // command line flags, parsed by Main.java. See the definitions / usage
  // there.
  private static String costModelName;
  //private static boolean onlyFinalReward;
  private static boolean verbose;
  private static boolean train;
  private static Query currentQuery;

  public static ArrayList<Query> trainQueries = null;
  public static ArrayList<Query> testQueries = null;
  // testing if features were set correctly
  public MyMetadataQuery mq;

  /*
  *************************************************************************
  *************************************************************************
                                  Methods
  *************************************************************************
  *************************************************************************
  */

  /* @dbUrl
   * @plannerTypes
   * @dataset
   */
  public QueryOptExperiment(String dbUrl, ArrayList<PLANNER_TYPE>
      plannerTypes, int port,
      boolean verbose, String costModelName,
      Params params) throws SQLException
  {
    this.params = params;
    // starts in training mode
    //this.train = true;
    this.costModelName = costModelName;
    this.verbose = verbose;
    this.dbUrl = dbUrl;
    this.conn = (CalciteConnection) DriverManager.getConnection(dbUrl);
    DbInfo.init(conn);
    this.zmq = new ZeroMQServer(port, verbose);
    this.plannerTypes = plannerTypes;
    volcanoPlanners = new ArrayList<Planner>();
    this.mq = MyMetadataQuery.instance();

    // Initialize all the volcanoPlanners we should need
    for (PLANNER_TYPE t  : plannerTypes) {
      Frameworks.ConfigBuilder bld = getDefaultFrameworkBuilder();
      bld.programs(MyJoinUtils.genJoinRule(t.getRules(), 1));
      Planner planner = Frameworks.getPlanner(bld.build());
      volcanoPlanners.add(planner);
    }

    // let's initialize cardinalities dictionary to given file
    if (params.cardinalitiesModel.equals("file")) {
      File file = new File(params.cardinalitiesModelFile);
      String jsonStr = null;
      try {
        jsonStr = FileUtils.readFileToString(file, "UTF-8");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
      setCardinalities(jsonStr);
    }
  }

  // static setter functions
  public static void setTrainMode(boolean mode) {
    params.train = mode;
  }

  // static getter functions.
  // FIXME: explain why we have so many of these
  public static String getCostModelName() {
    return costModelName;
  }

  public static CalciteConnection getCalciteConnection() {
    return conn;
  }

  public static Params getParams() {
    return params;
  }

  public static Query getCurrentQuery() {
    return currentQuery;
  }

  public static ZeroMQServer getZMQServer() {
    return zmq;
  }

  public static void setQueries(String mode, HashMap<String, String> queries)
  {
    ArrayList<Query> queryList = null;
    if (mode.equals("train")) {
      System.out.println("setQueries mode: train");
      trainQueries = new ArrayList<Query>();
      queryList = trainQueries;
    } else {
      System.out.println("setQueries mode: test");
      testQueries = new ArrayList<Query>();
      queryList = testQueries;
    }
    try {
      for (String queryName : queries.keySet()) {
        queryList.add(new Query(queryName, queries.get(queryName)));
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    if (verbose) System.out.println("successfully setQueries!");
  }

  public class OptimizationJob implements Callable<RelNode>
  {
		String sql;
		String queryName;
    Planner optPlanner;
    public OptimizationJob(Query query)
    {
			this.sql = query.sql;
      this.queryName = query.queryName;
      PLANNER_TYPE t = PLANNER_TYPE.LOpt;
      //PLANNER_TYPE t = PLANNER_TYPE.EXHAUSTIVE;
      //PLANNER_TYPE t = PLANNER_TYPE.LEFT_DEEP;

      try {
        Frameworks.ConfigBuilder bld = getDefaultFrameworkBuilder();
        bld.programs(MyJoinUtils.genJoinRule(t.getRules(queryName), 1));
        optPlanner = Frameworks.getPlanner(bld.build());
      } catch (Exception e) {
        System.out.println(e);
        System.exit(-1);
      }
    }

    @Override
    public RelNode call() throws Exception {
      optPlanner.close();
      optPlanner.reset();
      RelNode node = null;
      try {
        SqlNode sqlNode = optPlanner.parse(sql);
        SqlNode validatedSqlNode = optPlanner.validate(sqlNode);
        node = optPlanner.rel(validatedSqlNode).project();
      } catch (Exception e) {
        System.out.println(e);
        System.out.println("failed in getting Relnode from  " + sql);
        System.exit(-1);
      }

      DbInfo.setCurrentQueryVisibleFeatures(node);
      RelTraitSet traitSet = optPlanner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);

      try {
        // using the default volcano planner.
        long start = System.currentTimeMillis();
        RelNode optNode = optPlanner.transform(0, traitSet, node);
        return optNode;
      } catch (Exception e) {
        System.out.println(e);
        System.exit(-1);
      }
      // would never reach this.
      return null;
	  }
  }

  /* Optimizes all the nodes specified in trainQueries in parallel.
   */
  private ArrayList<RelNode> optimizeNodesParallel()
  {
    ArrayList<RelNode> nodes = new ArrayList<RelNode>();
    ExecutorService executor = Executors.newFixedThreadPool(10);
    List<Future<RelNode>> results = new ArrayList<Future<RelNode>>();

    // TODO: this should happen in parallel
    for (int i = 0; i < trainQueries.size(); i++)
    {
      Query curQuery = trainQueries.get(i);
      results.add(executor.submit(new OptimizationJob(curQuery)));
    }

    for (int i = 0; i < results.size(); i++) {
      Future<RelNode> result = results.get(i);
      try {
        RelNode node = result.get(600, TimeUnit.SECONDS);
        nodes.add(node);
      } catch (Exception e) {
          // interrupts if there is any possible error
          result.cancel(true);
          e.printStackTrace();
          System.out.println("Error while running search algorithm, so crashing");
          System.exit(-1);
      }
    }
    // FIXME: is it worth reusing these executors through the experiment?
    try {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
    return nodes;
  }

  public void startTestCardinalities() throws Exception
  {
    System.out.println("testCardinalities starting");
    if (params.python) zmq.waitForClientTill("getAttrCount");

    // checked in JoinOrder rules, in the RL settings, we update the
    // currentQuery structure with the appropriate order and other information
    // etc.
    currentQuery = null;

    HashMap<String, Double> allOptCosts = new HashMap<String, Double>();
    //HashMap<String, Double> allOptCosts = loadHM(allOptCostsPersistentFN);

    // FIXME: need to put up break conditions
    while (true) {
      if (trainQueries == null) {
        zmq.waitForClientTill("setQueries");
      }

      // the python side should set cardinalities etc. at this point
      zmq.waitForClientTill("setCardinalities");
      zmq.waitForClientTill("startTestCardinalities");
      ArrayList<RelNode> estNodes = optimizeNodesParallel();

      // the python side should set cardinalities etc. at this point
      zmq.waitForClientTill("setCardinalities");

      // only need to compute these IF true cardinality based costs haven't
      // been computed before.

      ArrayList<RelNode> optNodes = null;
      String query0Name = trainQueries.get(0).queryName;
      // FIXME: assuming the whole batch of trainQueries is passed in together
      // at least the first time.
      if (allOptCosts.get(query0Name) == null) {
        optNodes = optimizeNodesParallel();
      }

      // for every batch of estimations, we want to recompute the cost. But for
      // the true estimates, we should only need to do it once, so not creating
      // a new HashMap for zmq.optCosts
      zmq.estCosts = new HashMap<String, Double>();
      zmq.optCosts = new HashMap<String, Double>();

      for (int i = 0; i < estNodes.size(); i++) {
        String queryName = trainQueries.get(i).queryName;
        mq.setQueryName(queryName);
        Double estCost = computeCost(mq, estNodes.get(i)).getCost();
        Double optCost;
        if (optNodes != null) {
          optCost = computeCost(mq, optNodes.get(i)).getCost();
          allOptCosts.put(queryName, optCost);
        } else {
          optCost = allOptCosts.get(queryName);
        }
        zmq.estCosts.put(queryName, estCost);
        zmq.optCosts.put(queryName, optCost);
      }

      // set variables in zmq
      zmq.waitForClientTill("getEstCardinalityCosts");
      zmq.waitForClientTill("getOptCardinalityCosts");

      if (false) {
        break;
      }
      saveUpdatedHM(allOptCostsPersistentFN, allOptCosts);
    }
  }

  /* FIXME: finalize queries semantics AND write explanation.
   * FIXME: used for both train and test, change name to reflect that.
   */
  public void start() throws Exception
  {
    // we will treat queries as a pool of sample data. After every reset, we
    // choose a new
    int numSuccessfulQueries = 0;
    int numFailedQueries = 0;
    // start a server, and wait for a command.
    if (params.python) zmq.waitForClientTill("getAttrCount");
    int nextQuery = -1;
    ArrayList<Query> queries;
    // TODO: ugh clean up the way i handle ordering etc.
    boolean alreadyTesting = false;
    while (true) {

      if (trainQueries == null) {
        zmq.waitForClientTill("setQueries");
      }

      if (verbose) System.out.println("total queries: " + trainQueries.size());
      // at this point, all the other planners would have executed on the
      // current query as well, so all the stats about it would be updated in
      // the Query struct.
      if (params.python) zmq.waitForClientTill("getQueryInfo");
      if (params.python) zmq.waitForClientTill("reset");
      if (params.train) {
        alreadyTesting = false;
        queries = trainQueries;
        // FIXME: is deterministic order ok always?
        nextQuery = (nextQuery + 1) % queries.size();
      } else {
        queries = testQueries;
        if (alreadyTesting) {
          nextQuery = (nextQuery + 1) % queries.size();
        } else {
          nextQuery = 0;
          alreadyTesting = true;
        }
      }
      // FIXME: simplify this
      Query query = queries.get(nextQuery);

      if (verbose) System.out.println("nextQuery is: " + nextQuery);

      String sqlQuery = query.sql;
      currentQuery = query;
      zmq.sqlQuery = sqlQuery;
      if (zmq.END) break;
      if (volcanoPlanners.size() == 0) {
        System.out.println("no planners specified");
        break;
      }
      zmq.reset = false;

      for (int i = 0; i < volcanoPlanners.size(); i++) {
        try {
          boolean success = planAndExecuteQuery(query, i);
        } catch (Exception e) {
          String plannerName = plannerTypes.get(i).name();
          e.printStackTrace();
          query.costs.put(plannerName, -1.00);
          System.out.println("failed in planAndExecute for " + plannerName +
              " for query number " + nextQuery);
          System.exit(-1);
        }
      }
      if (params.verifyResults) {
        if (!query.verifyResults()) {
          System.err.println("verifying results failed");
          // just exit because what else to do?
          System.exit(-1);
        }
      }
    }
  }

  private MyCost computeCost(RelMetadataQuery mq, RelNode node) {
    return (MyCost) ((MyMetadataQuery) mq).getCumulativeCost(node);
  }

  private void execPlannerOnDB(Query query, String plannerName, RelNode node)
  {
    ArrayList<Long> savedRTs = query.dbmsAllRuntimes.get(plannerName);
    if (savedRTs == null) {
      savedRTs = new ArrayList<Long>();
    }

    if (savedRTs.size() >= params.numExecutionReps
          && !plannerName.equals("RL")) {
      // don't re-execute and waste everyone's breathe
      return;
    }
    MyUtils.ExecutionResult result = null;
    for (int i = 0; i < 2; i++) {
      if (plannerName.equals("postgres")) {
        // run N times, and store the average
        result = MyUtils.executeSql(query.sql, false,
            params.clearCache);
      } else {
        result = MyUtils.executeNode(node, false, params.clearCache);
      }
      System.out.println(plannerName + " took " + result.runtime + "ms" + " for " + query.queryName);
      savedRTs.add(result.runtime);
    }
    query.dbmsAllRuntimes.put(plannerName, savedRTs);
  }

  private boolean planAndExecuteQuery(Query query, int plannerNum)
    throws Exception
  {
    Planner planner = volcanoPlanners.get(plannerNum);
    String plannerName = plannerTypes.get(plannerNum).name();
    //System.out.println("planAndExecuteQuery with: " + plannerName);
    // Need to do this every time we reuse a planner
    planner.close();
    planner.reset();
    // first, have we already run this planner + query combination before?
    // In that case, we have no need to execute it again, as the result
    // will be stored in the Query object. Never do caching for RL since
    // the plans are constantly changing.
    if (!plannerName.equals("RL") &&
            !params.recomputeFixedPlanners
            && !params.execOnDB)
    {
      Double cost = query.costs.get(plannerName);
      // if we already executed this query, and nothing should change for the
      // deterministic planners
      if (cost != null) return true;
    }
    RelNode node = null;
    try {
      SqlNode sqlNode = planner.parse(query.sql);
      SqlNode validatedSqlNode = planner.validate(sqlNode);
      node = planner.rel(validatedSqlNode).project();
    } catch (Exception e) {
      System.out.println(e);
      System.out.println("failed in getting Relnode from  " + query.sql);
      System.exit(-1);
    }

    DbInfo.setCurrentQueryVisibleFeatures(node);
    // very important to do the replace EnumerableConvention thing for
    // mysterious reasons
    RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);

    try {
      // using the default volcano planner.
      long start = System.currentTimeMillis();
      String origPlan = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
      //System.out.println(origPlan);
      RelNode optimizedNode = planner.transform(0, traitSet,
              node);
      long planningTime = System.currentTimeMillis() - start;
      if (verbose) System.out.println("planning time: " +
          planningTime);
      //System.out.println("planning time: " +
          //planningTime + " for " + plannerName);
      RelOptCost optCost = computeCost(mq, optimizedNode);
      // Time to update the query with the current results
      query.costs.put(plannerName, ((MyCost) optCost).getCost());
      if (verbose) System.out.println("optimized cost for " + plannerName
          + " is: " + optCost);

      String optPlan = RelOptUtil.dumpPlan("", optimizedNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
      query.plans.put(plannerName, optPlan);
      // slightly complicated because we need information from multiple places
      // in the joinOrder struct
      MyUtils.JoinOrder joinOrder = query.joinOrders.get(plannerName);
      joinOrder = MyUtils.updateJoinOrder(optimizedNode, joinOrder);
      query.joinOrders.put(plannerName, joinOrder);
      query.planningTimes.put(plannerName, planningTime);

      if (params.getSqlToExecute) {
        String sqlToExecute = MyUtils.getSqlToExecute(optimizedNode);
        query.executedSqls.put(plannerName, sqlToExecute);
      }

      if (params.execOnDB) {
        execPlannerOnDB(query, plannerName, optimizedNode);
      }
    } catch (Exception e) {
      // it is useful to throw the error here to see what went wrong..
        throw e;
    }

    // FIXME: to execute on postgres, just execute plain sql
    if (params.execOnDB && !params.train) {
      execPlannerOnDB(query, "postgres", node);
    }
    return true;
  }

  private Frameworks.ConfigBuilder getDefaultFrameworkBuilder() throws
      SQLException
  {
    // build a FrameworkConfig using defaults where values aren't required
    Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
    configBuilder.defaultSchema(conn.getRootSchema().getSubSchema(conn.getSchema()));
    // seems the simplest to get it working. Look at older commits for
    // other variants on this
    configBuilder.parserConfig(SqlParser.configBuilder()
                                .setLex(Lex.MYSQL)
                                .build());
    // FIXME: not sure if all of these are required?!
    final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);
    configBuilder.traitDefs(traitDefs);
    configBuilder.context(Contexts.EMPTY_CONTEXT);
    configBuilder.costFactory(MyCost.FACTORY);
    return configBuilder;
  }

  /* @node: the node to be executed over the jdbc connection (this.conn).
   * @ret: The cardinality of the result.
   * Note: this has ONLY been tested when node is a base table relation (SCAN or
   * FILTER->SCAN on a table)
   * FIXME: This seems to fail if the query takes too long to execute...
   * (e.g., SELECT * from cast_info, or SELECT * from movie_info)
   */
  public static Double getTrueCardinality(RelNode node)
  {
    CalciteConnection curConn;
    PreparedStatement ps = null;
    ResultSet res = null;
    Double cardinality = null;
    try {
      // recreating the connection should also work equally well.
      //curConn = (CalciteConnection) DriverManager.getConnection(dbUrl);
      curConn = conn;
      RelRunner runner = curConn.unwrap(RelRunner.class);
      ps = runner.prepare(node);
      long start = System.currentTimeMillis();
      res = ps.executeQuery();
      long end = System.currentTimeMillis();
      long total = end - start;
      System.out.println("execution time: " + total);
      if (res != null) {
        cardinality = 0.00;
        while (res.next()) {
          cardinality += 1.00;
        }
      } else {
        // something went wrong? should we fail?
        System.err.println("something went wrong while computing cardinality!!!");
        cardinality = 0.00;
      }
    } catch (SQLException e) {
      System.out.println("caught exception while trying to find cardinality of subquery");
      System.out.println(e);
      e.printStackTrace();
      //System.exit(-1);
      // FIXME: temp.
      return 100.00;
    }
    try {
      ps.close();
      res.close();
    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
      // no good way to handle this (?)
      //System.exit(-1);
      // FIXME: temp.
      return 100.00;
    }
    if (verbose) System.out.println("true cardinality was: " + cardinality);
    return cardinality;
  }

  public static void setCardinalities(String jsonStr) {
      Gson gson = new Gson();
      params.cardinalities = gson.fromJson(jsonStr,
          new TypeToken<HashMap<String, HashMap<String, Long>>>() {}.getType());

  }

  /// FIXME: temporary stuff, should be separate class for persistent things
  public void saveUpdatedHM(String fileName, HashMap<String,Double>
      updatedCosts) {
    System.out.println("saveUpdatedHM " + fileName);
    HashMap<String, Double> oldCosts = (HashMap) loadHM(fileName);
    HashMap<String, Double> newCosts = new HashMap<String, Double>();
    if (oldCosts != null){
      // ignore this guy, file probably didn't exist.
      newCosts.putAll(oldCosts);
    }
    newCosts.putAll(updatedCosts);
    saveObj(newCosts, fileName);
  }

  // FIXME: make these general purpose
  private void saveObj(Serializable obj, String fileName)
  {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(fileName)
			);
			oos.writeObject(obj);
			oos.flush();
			oos.close();
		} catch (Exception e) {
			System.out.println(e);
		}
  }

  public HashMap<String,Double> loadHM(String fileName) {
    try {
      FileInputStream fis = new FileInputStream(fileName);
      ObjectInputStream ois = new ObjectInputStream(fis);
      HashMap<String, Double> costs = (HashMap) ois.readObject();
      ois.close();
      System.out.println("loadHM successfully found file to load");
      return costs;
    } catch (Exception e) {
      System.out.println(e);
    }
    HashMap<String, Double> costs = new HashMap<String, Double>();
    return costs;
  }
}
