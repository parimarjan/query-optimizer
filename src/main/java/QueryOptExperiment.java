import java.sql.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.tools.*;
import java.io.*;
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
import java.util.concurrent.ThreadLocalRandom;

// experimental:
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.*;
//import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.ImmutableBitSet;
import java.util.concurrent.TimeUnit;

/* Will contain all the parameters / data etc. to drive one end to end
 * experiment.
 */
public class QueryOptExperiment {

    private static CalciteConnection conn;
    private static String dbUrl;

    public enum PLANNER_TYPE
    {
        ORIG_JOIN_ORDER,
        EXHAUSTIVE,
        LOpt,
        RANDOM,
        DEBUG,
        BUSHY,
        RL,
        LEFT_DEEP;

        // using com.google.ImmutableList because we can't declare ArrayList in
        // static context.
        public static final ImmutableList<RelOptRule> ORIG_JOIN_ORDER_RULES =
            ImmutableList.of(FilterJoinRule.FILTER_ON_JOIN,
						        ProjectMergeRule.INSTANCE);

        // according to comments in Programs.heuristicJoinOrder, if
        // we add JoinCommutRule + JoinPushThroughJoinRule +
        // JoinAssociateRule, then we should get exhaustive search.
        // This makes sense.
        // public static final ImmutableList<RelOptRule> EXHAUSTIVE_RULES =
            //ImmutableList.of(JoinCommuteRule.INSTANCE,
                    //JoinAssociateRule.INSTANCE,
                    //JoinPushThroughJoinRule.RIGHT,
                    //JoinPushThroughJoinRule.LEFT,
                    //FilterJoinRule.FILTER_ON_JOIN,
										//ProjectMergeRule.INSTANCE);
        public static final ImmutableList<RelOptRule> EXHAUSTIVE_RULES =
            ImmutableList.of(ExhaustiveJoinOrderRule.INSTANCE,
                             FilterJoinRule.FILTER_ON_JOIN,
                             ProjectMergeRule.INSTANCE);
        public static final ImmutableList<RelOptRule> LEFT_DEEP_RULES =
            ImmutableList.of(LeftDeepJoinOrderRule.INSTANCE);


        public static final ImmutableList<RelOptRule> LOPT_RULES =
            ImmutableList.of(MyLoptOptimizeJoinRule.INSTANCE,
                            FilterJoinRule.FILTER_ON_JOIN,
                            ProjectMergeRule.INSTANCE);

        public static final ImmutableList<RelOptRule> RANDOM_RULES =
            ImmutableList.of(JoinOrderTest.INSTANCE);

        public static final ImmutableList<RelOptRule> DEBUG_RULES =
            ImmutableList.of(MyLoptOptimizeJoinRule.INSTANCE);

        public static final ImmutableList<RelOptRule> BUSHY_RULES =
            ImmutableList.of(MultiJoinOptimizeBushyRule.INSTANCE);

				// Note: need the second projection rule as otherwise the optimized
				// node from the joins was projecting all the fields before projecting it down to
				// only the selected fields
				public static final ImmutableList<RelOptRule> RL_RULES = ImmutableList.of(
            RLJoinOrderRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            ProjectMergeRule.INSTANCE);

        // FIXME: not sure if we need to add other rules - like we
        // could add all of the Programs.RULE_SET here, and remove the
        // exhaustive rules above (that was done in heuristicJoinOrder)

        public ImmutableList<RelOptRule> getRules() {
          switch(this){
            case ORIG_JOIN_ORDER:
              return ORIG_JOIN_ORDER_RULES;
            case EXHAUSTIVE:
              return EXHAUSTIVE_RULES;
            case LOpt:
              return LOPT_RULES;
            case RANDOM:
              return RANDOM_RULES;
            case DEBUG:
              return DEBUG_RULES;
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
    //private ArrayList<Planner> hepPlanners;

    public enum QUERIES_DATASET
    {
        JOB,
        ORIG_JOB,
        SIMPLE;
        public String getDatasetPath() {
            switch(this){
                case ORIG_JOB:
                    return "./orig-join-order-benchmark/";
                case JOB:
                    return "./join-order-benchmark/";
                case SIMPLE:
                    return "./simple-queries/";
                default:
                    return "";
            }
        }

        public static QUERIES_DATASET getDataset(String ds)
        {
          switch (ds) {
            case "JOB":
              return JOB;
            case "SIMPLE":
              return SIMPLE;
            default:
              System.err.println("unsupported dataset");
              System.exit(-1);
          }
          return null;
        }
    }

    public ArrayList<String> allSqlQueries;
    public static ZeroMQServer zmq;
    private HashMap<String, HashMap<String, String>> resultVerifier;

    private boolean executeOnDB;
    private static String costModelName;
    private static boolean onlyFinalReward;
    private static boolean verbose;
    private static boolean train;

    // FIXME: temporary. Get rid of this soon.
    private static boolean useSavedCosts = false;
    private static String currentQuery;

    // so we can use the same instance in various rules
    //private static MyMetadataQuery mq;

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
    public QueryOptExperiment(String dbUrl, ArrayList<PLANNER_TYPE> plannerTypes, QUERIES_DATASET queries, int port, boolean onlyFinalReward, boolean verbose, boolean train, String costModelName, boolean executeOnDB) throws SQLException {
        // FIXME: make this as a variable arg.
        this.executeOnDB = executeOnDB;
        this.train = train;
        this.costModelName = costModelName;
        //this.costModelName = "CM2";
        this.onlyFinalReward = onlyFinalReward;
        this.verbose = verbose;
        this.dbUrl = dbUrl;
        this.conn = (CalciteConnection) DriverManager.getConnection(dbUrl);
        DbInfo.init(conn);
        this.zmq = new ZeroMQServer(port, verbose);

        this.plannerTypes = plannerTypes;
        volcanoPlanners = new ArrayList<Planner>();

        // Initialize all the volcanoPlanners we should need
        for (PLANNER_TYPE t  : plannerTypes) {
            Frameworks.ConfigBuilder bld = getDefaultFrameworkBuilder();
            bld.programs(MyJoinUtils.genJoinRule(t.getRules(), 1));
            Planner planner = Frameworks.getPlanner(bld.build());
            volcanoPlanners.add(planner);
        }

        // load in the sql queries dataset
        allSqlQueries = new ArrayList<String>();
        resultVerifier = new HashMap<String, HashMap<String, String>>();
        File dir = new File(queries.getDatasetPath());
        File[] listOfFiles = dir.listFiles();
        for (File f : listOfFiles) {
            // FIXME: use regex to avoid index files etc.
            if (f.getName().contains(".sql")) {
                String sql;
                try {
                    sql = FileUtils.readFileToString(f);
                } catch (Exception e) {
                    System.out.println("could not read file " + f.getName());
                    continue;
                };
                // FIXME: parse the sql to avoid calcite errors.
                String escapedSql = queryRewriteForCalcite(sql);
                allSqlQueries.add(escapedSql);
                //if (allSqlQueries.size() >= 8) {
                  //System.out.println(f.getName());
                //}
                resultVerifier.put(escapedSql, new HashMap<String, String>());
            }
        }
        //mq = MyMetadataQuery.instance();
    }


    /* This function will act as zeromq server controlled by an agent on the
     * client side (currently an RL agent in Python), using standard openAI gym
     * semantics.
     */
    public void train(ArrayList<Integer> queries) throws Exception
    {
      // we will treat queries as a pool of sample data. After every reset, we
      // choose a new
      int numSuccessfulQueries = 0;
      int numFailedQueries = 0;
      zmq.curQuerySet = queries;
      // start a server, and wait for a command.
      zmq.waitForClientTill("getAttrCount");
      int nextQuery = -1;
      while (true) {
        if (train ) {
          nextQuery = ThreadLocalRandom.current().nextInt(0, queries.size());
        } else {
          nextQuery = (nextQuery + 1) % queries.size();
        }
        if (verbose) System.out.println("nextQuery is: " + nextQuery);
        String query = allSqlQueries.get(queries.get(nextQuery));
        currentQuery = query;
        zmq.query = query;

        zmq.waitForClientTill("reset");
        if (zmq.END) break;
        zmq.reset = false;
        for (int i = 0; i < volcanoPlanners.size(); i++) {
          try {
            boolean success = planAndExecuteQuery(query, i);
            if (plannerTypes.get(i).name().equals("EXHAUSTIVE")) {
              numSuccessfulQueries += 1;
            }
          } catch (Exception e) {
            //System.out.println(query);
            String plannerName = plannerTypes.get(i).name();
            if (plannerName.equals("EXHAUSTIVE")) {
              numFailedQueries += 1;
            }
            System.out.println("failed in planAndExecute for " + plannerName + " for query number " + nextQuery);
            //System.out.println(e);
            //e.printStackTrace();
            //zmq.optimizedCosts.get(query).put(plannerName, 0.00);
            //throw e;
          }
        }
        if (executeOnDB) {
          if (!verifyResults(query)) {
            System.err.println("verifying results failed");
            //System.exit(-1);
          } else {
            System.out.println("verifying results succeeded");
          }
        }
      }
    }

    public static String getCostModelName() {
      return costModelName;
    }

    public static CalciteConnection getCalciteConnection() {
      return conn;
    }

    public static boolean onlyFinalReward() {
      return onlyFinalReward;
    }

    //public static MyMetadataQuery getMetadataQuery() {
      //return mq;
    //}
    public static String getCurrentQuery() {
      return currentQuery;
    }

    public static ZeroMQServer getZMQServer() {
      return zmq;
    }

    private RelOptCost getCost(RelMetadataQuery mq, RelNode node) {
      return ((MyMetadataQuery) mq).getCumulativeCost(node);
    }

    private boolean planAndExecuteQuery(String query, int plannerNum)
      throws Exception
    {
        Planner planner = volcanoPlanners.get(plannerNum);
        String plannerName = plannerTypes.get(plannerNum).name();
        //System.out.println("planner name: " + plannerName);
        // doing this at the start because there are many possible exit
        // points because of various failures.
        planner.close();
        planner.reset();
        // first, have we already run this planner + query combination before?
        // In that case, we have no need to execute it again, as the result
        // will be stored in the zmq object.
        // FIXME: better storage system here.
        // FIXME: add a separate storage layer?
        HashMap<String, Double> planCostMap = zmq.optimizedCosts.get(query);
        if (planCostMap == null) {
          // this query has not been seen so far.
          zmq.optimizedCosts.put(query, new HashMap<String, Double>());
        } else if (!executeOnDB && useSavedCosts) {
          // for RL, or if we need to executeOnDb, we always continue executing.
          //if (plannerName.equals("EXHAUSTIVE")) {
          if (!plannerName.equals("RL")) {
            // let's check if this planner has been seen for this query.
            Double cost = planCostMap.get(plannerName);
            if (cost != null) {
              // have already run this, so don't have to do it again.
              System.out.println("saved optimized cost for " + plannerName + " is: " + cost);
              return true;
            }
          }
        }

        RelNode node = null;
        try {
            SqlNode sqlNode = planner.parse(query);
            SqlNode validatedSqlNode = planner.validate(sqlNode);
            node = planner.rel(validatedSqlNode).project();
        } catch (SqlParseException e) {
            System.out.println(e);
            System.out.println("failed to parse: " + query);
            return false;
            // throw e;
        } catch (ValidationException e) {
            System.out.println(e);
            System.out.println("failed to validate: " + query);
            return false;
            //throw e;
        } catch (Exception e) {
            System.out.println(e);
            System.out.println("failed in getting Relnode from  " + query);
            return false;
            //System.exit(-1);
        }
        DbInfo.setCurrentQueryVisibleFeatures(node);
        // testing if features were set correctly
        ImmutableBitSet bs = DbInfo.getCurrentQueryVisibleFeatures();
        MyMetadataQuery mq = MyMetadataQuery.instance();
        /// very important to do the replace EnumerableConvention thing
        RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
        try {
            // FIXME: check if this might actually be working now.
            //tryHepPlanner(node, traitSet, mq);
            // using the default volcano planner.
            long start = System.currentTimeMillis();
            RelNode optimizedNode = planner.transform(0, traitSet,
                    node);
            String optPlan = RelOptUtil.dumpPlan("optimized plan: \n", optimizedNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
            RelOptCost optCost = getCost(mq, optimizedNode);
            //System.out.println("optCost: " + optCost);
            if (verbose) System.out.println("optimized cost for " + plannerName + " is: " + optCost);
            //System.out.println("optimized cost for " + plannerName + " is: " + optCost);
            if (verbose) System.out.println("planning time: " +
                (System.currentTimeMillis()- start));
            ZeroMQServer zmq = getZMQServer();
            HashMap<String, Double> updCosts = zmq.optimizedCosts.get(query);
            updCosts.put(plannerName, ((MyCost) optCost).getCost());
            zmq.optimizedCosts.put(query, updCosts);
            // debug
            if (!plannerName.equals("RL")) {
              zmq.saveUpdatedCosts();
            }
            // FIXME: update plans: do this the same way costs are updated.
            //HashMap<String, String> updPlans = zmq.optimizedPlans.get(query);
            //updPlans.put(plannerName, optPlan);
            //zmq.optimizedPlans.put(query, updPlans);

            if (executeOnDB) {
              System.out.println("going to execute " + plannerName);
              //String results = executeNode(optimizedNode, conn);
              String results = executeNode(optimizedNode);
              resultVerifier.get(query).put(plannerName, results);
            }
        } catch (Exception e) {
            // it is useful to throw the error here to see what went wrong..
            throw e;
        }
        return true;
    }

    private Frameworks.ConfigBuilder getDefaultFrameworkBuilder() throws
        SQLException
        {
        // build a FrameworkConfig using defaults where values aren't required
        Frameworks.ConfigBuilder configBuilder = Frameworks.newConfigBuilder();
        configBuilder.defaultSchema(conn.getRootSchema().getSubSchema(conn.getSchema()));
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder();
        // now we can set it to avoid upper casing and stuff
        // It does not have postgres specific casing rules, but JAVA rules seem
        // most sensible, and works with parsing the queries we got.
        //parserBuilder.setQuotedCasing(Lex.JAVA.quotedCasing)
                     //.setUnquotedCasing(Lex.JAVA.unquotedCasing)
                     //.setQuoting(Lex.JAVA.quoting)
                     //.setCaseSensitive(Lex.JAVA.caseSensitive);
        //configBuilder.parserConfig(parserBuilder.build());

        configBuilder.parserConfig(SqlParser.configBuilder()
                                    .setLex(Lex.MYSQL)
                                    .build());


        // FIXME: experimental stuff
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
        traitDefs.add(ConventionTraitDef.INSTANCE);
        //traitDefs.add(EnumerableConvention.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        configBuilder.traitDefs(traitDefs);
        configBuilder.context(Contexts.EMPTY_CONTEXT);
        configBuilder.costFactory(MyCost.FACTORY);
        return configBuilder;
    }

    private void tryHepPlanner(RelNode node, RelTraitSet traitSet, RelMetadataQuery mq) {
        // all of these rules are really important to get any performance
        final HepProgram hep = new HepProgramBuilder()
                .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
                .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
                .addRuleInstance(RLJoinOrderRule.INSTANCE)
                .addRuleInstance(ProjectMergeRule.INSTANCE)
                .build();

        // old attempt: did not have rule order / JoinToMultiJoinRule
        //HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        //final HepProgram hep = hepProgramBuilder.build();
        HepPlanner hepPlanner = new HepPlanner(hep);
        hepPlanner.setRoot(node);
        // TODO: does not look like adding rules here makes much difference,
        // just addRuleInstance on builder seems to work fine.
        // hepPlanner.addRule(FilterMergeRule.INSTANCE);
        hepPlanner.changeTraits(node, traitSet);
        //hepPlanner.addRule(RLJoinOrderRule.INSTANCE);
        //hepPlanner.addRule(ProjectMergeRule.INSTANCE);

        // TODO: metadata stuff. Doesn't seem to make a difference / be needed
        // right now.
        //final RelMetadataProvider provider = node.getCluster().getMetadataProvider();
        //final ArrayList<RelMetadataProvider> list = new ArrayList<RelMetadataProvider>();
        //list.add(provider);
        //hepPlanner.registerMetadataProviders(list);
        //System.out.println("registered metadata provider with hep planner");
        //final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
        //node.accept(new MetaDataProviderModifier(cachingMetaDataProvider));

        long start = System.currentTimeMillis();
        RelNode hepTransform = hepPlanner.findBestExp();
        System.out.println("planning time: " + (System.currentTimeMillis()-
              start));
        //System.out.println(RelOptUtil.dumpPlan("optimized hep plan:", hepTransform, SqlExplainFormat.TEXT, SqlExplainLevel.NO_ATTRIBUTES));

        //System.out.println("hep optimized toString is: " +
            //RelOptUtil.toString(hepTransform));
        //System.out.println("hep optimized cost is: " + mq.getCumulativeCost2(hepTransform));
        //System.out.println("executing hep optimized node...");
        //executeNode(hepTransform, conn);
        executeNode(hepTransform);
    }

    public static Double getTrueCardinality(RelNode node)
    {
      CalciteConnection curConn;
      PreparedStatement ps = null;
      ResultSet res = null;
      Double cardinality = null;
      System.out.println(RelOptUtil.dumpPlan("plan for node being executed:", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
      try {
        //curConn = (CalciteConnection) DriverManager.getConnection(dbUrl);
        curConn = conn;
        RelRunner runner = curConn.unwrap(RelRunner.class);
        ps = runner.prepare(node);
        long start = System.currentTimeMillis();
        System.out.println("executing node");
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
        System.exit(-1);
      }
      try {
        ps.close();
        res.close();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        // no good way to handle this (?)
        System.exit(-1);
      }
      System.out.println("true cardinality was: " + cardinality);
      return cardinality;
    }

    public static String executeNode(RelNode node)
    {
      String combinedResults = "";
      CalciteConnection curConn;
      try {
        curConn = (CalciteConnection) DriverManager.getConnection(dbUrl);
        //curConn = conn;
        //System.out.println(RelOptUtil.dumpPlan("plan for node being executed:", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
        System.out.println("in execute node!");
        System.out.println("nodeToString:\n " + RelOptUtil.toString(node));
        RelRunner runner = curConn.unwrap(RelRunner.class);
        System.out.println("after curConn.unwrap!");
        PreparedStatement ps = runner.prepare(node);
        System.out.println(ps);
        ps.setQueryTimeout(10);
        long start = System.currentTimeMillis();
        ResultSet res = null;
        System.out.println("executing node");
        //res = ps.executeQuery();
        try {
          res = ps.executeQuery();
        } catch (Exception e) {
          curConn.close();
          System.out.println("going to execute it again");
          curConn = (CalciteConnection) DriverManager.getConnection(dbUrl);
          runner = curConn.unwrap(RelRunner.class);
          ps = runner.prepare(node);
          res = ps.executeQuery();
        }
        long end = System.currentTimeMillis();
        long total = end - start;
        System.out.println("execution time: " + total);
        ResultSetMetaData rmd = res.getMetaData();
        int num_columns = rmd.getColumnCount();
        System.out.println("num columns " + num_columns);
        int curLine = 0;
        while (res.next()) {
          if (curLine <= 1000) {
            //System.out.println(res.getString(0));
            for (int i = 1; i < num_columns+1; i++) {
              //System.out.println("column: " + i + ": " + res.getString(i));
              combinedResults += res.getString(i);
            }
          } else break;
          curLine += 1;
        }
        System.out.println("totalLines = " + curLine);
        //curConn.close();
      } catch (SQLException e) {
        System.out.println("caught exeception while executing query");
        System.out.println(e);
        e.printStackTrace();
        //System.exit(-1);
      }
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        System.err.println("exception while sleeping");
      }
      return Integer.toString(combinedResults.hashCode());
    }

    private void printInfo(RelNode node) {
        Set<CorrelationId> setIds = node.getVariablesSet();
        System.out.println("num setIds: " + setIds.size());
        System.out.println("rel class: " + node.getClass().getName());
        System.out.println("rel convention: " + node.getConvention());
        System.out.println("rel query class: " + node.getQuery().getClass().getName());
        //System.out.println(RelOptUtil.toString(node));
        System.out.println("digest: " + node.recomputeDigest());
        RelDataType dt = node.getRowType();
        System.out.println("dt.toString: " + dt.toString());

        if (node instanceof LogicalJoin) {
            LogicalJoin lnode = (LogicalJoin) node;
            System.out.println("systemFieldList size: " + lnode.getSystemFieldList().size());
        }
        for (RelNode inp : node.getInputs()) {
            System.out.println("next input");
            printInfo(inp);
        }
    }

    // FIXME: need to ensure this works fine in all cases.
    private String queryRewriteForCalcite(String query) {
        String newQuery = query.replace(";", "");
        newQuery = newQuery.replace("!=", "<>");
        // debugging purposes
        // FIXME: doesn't seem easy to add text here without running into
        // weird formatting issues (while it works just fine if we write the
        // same thing in the original queries)
        //newQuery = "\"explain\" " + newQuery;
        //newQuery = newQuery + " LIMIT 10";
        return newQuery;
    }

    private boolean verifyResults(String query) {
      HashMap<String, String> res = resultVerifier.get(query);
      // need to check that all the values in res must be the same!
      String rl_val = res.get("RL");
      for (String val : res.values()) {
        if (!rl_val.equals(val)) {
          return false;
        }
      }
      return true;
    }
}
