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

// experimental:
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.*;
//import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.ImmutableBitSet;

/* Will contain all the parameters / data etc. to drive one end to end
 * experiment.
 */
public class QueryOptExperiment {

    private CalciteConnection conn;

    public enum PLANNER_TYPE
    {
        EXHAUSTIVE,
        LOpt,
        RANDOM,
        DEBUG,
        BUSHY,
        RL,
        DQ;

        // using com.google.ImmutableList because we can't declare ArrayList in
        // static context.

        // according to comments in Programs.heuristicJoinOrder, if
        // we add JoinCommutRule + JoinPushThroughJoinRule +
        // JoinAssociateRule, then we should get exhaustive search.
        // This makes sense.
        public static final ImmutableList<RelOptRule> EXHAUSTIVE_RULES =
            ImmutableList.of(JoinCommuteRule.INSTANCE,
                    JoinAssociateRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT);

        public static final ImmutableList<RelOptRule> LOPT_RULES =
            ImmutableList.of(LoptOptimizeJoinRule.INSTANCE);

        public static final ImmutableList<RelOptRule> RANDOM_RULES =
            ImmutableList.of(JoinOrderTest.INSTANCE);

        public static final ImmutableList<RelOptRule> DEBUG_RULES =
            ImmutableList.of(LoptJoinOrderTest.INSTANCE);

        public static final ImmutableList<RelOptRule> BUSHY_RULES =
            ImmutableList.of(MultiJoinOptimizeBushyRule.INSTANCE);

				// Note: need the second projection rule as otherwise the optimized
				// node from the joins was projecting all the fields before projecting it down to
				// only the selected fields
				public static final ImmutableList<RelOptRule> RL_RULES = ImmutableList.of(
						RLJoinOrderRule.INSTANCE,
						ProjectMergeRule.INSTANCE);

        // FIXME: not sure if we need to add other rules - like we
        // could add all of the Programs.RULE_SET here, and remove the
        // exhaustive rules above (that was done in heuristicJoinOrder)

        public ImmutableList<RelOptRule> getRules() {
            switch(this){
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
                default:
                    return null;
            }
        }
    }
    /* actual planners generated using the above rules */
    private ArrayList<Planner> planners;

    public enum QUERIES_DATASET
    {
        JOB,
        SIMPLE;
        public String getDatasetPath() {
            switch(this){
                case JOB:
                    return "./join-order-benchmark/";
                case SIMPLE:
                    return "./simple-queries/";
                default:
                    return "";
            }
        }
    }

    public ArrayList<String> allSqlQueries;

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
    public QueryOptExperiment(String dbUrl, List<PLANNER_TYPE> plannerTypes, QUERIES_DATASET queries) throws SQLException {

        conn = (CalciteConnection) DriverManager.getConnection(dbUrl);
        DbInfo.init(conn);

        planners = new ArrayList<Planner>();
        allSqlQueries = new ArrayList<String>();

        // Initialize all the planners we should need
        for (PLANNER_TYPE t  : plannerTypes) {
            Frameworks.ConfigBuilder bld = getDefaultFrameworkBuilder();
            if (t == PLANNER_TYPE.EXHAUSTIVE) {
                // TODO: probably can use the same function as other cases too
                // and skip multijoin in genJoinRule.
                Program program = Programs.ofRules(t.getRules());
                List<Program> rules = new ArrayList<Program>();
                rules.add(program);
                bld.programs(rules);
            } else {
                // probably same statement can work with all types.
                //bld.programs(MyJoinUtils.genJoinRule(t.getRules(), 3));
                // FIXME temporary:
                bld.programs(MyJoinUtils.genJoinRule(t.getRules(), 1));
            }
            Planner planner = Frameworks.getPlanner(bld.build());
            planners.add(planner);
        }

        // load in the sql queries dataset
        File dir = new File(queries.getDatasetPath());
        File[] listOfFiles = dir.listFiles();
        for (File f : listOfFiles) {
            // FIXME: use regex to avoid index files etc.
            if (f.getName().contains(".sql")) {
            //if (f.getName().contains("3.sql")) {
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
            }
        }
    }

    /* Runs all the planners we have on all the given allSqlQueries, and collects
     * statistics about each run.
     */
    public void run(ArrayList<String> queries) throws Exception {
        int numSuccessfulQueries = 0;
        int numFailedQueries = 0;
        for (String query : queries) {
            //System.out.println(query);
            //if (numSuccessfulQueries == 1 || numFailedQueries == 1) break;
            for (Planner planner : planners) {
                // doing this at the start because there are many possible exit
                // points because of various failures.
                planner.close();
                planner.reset();
                RelNode node = null;
                try {
                    SqlNode sqlNode = planner.parse(query);
                    //System.out.println(sqlNode);
                    SqlNode validatedSqlNode = planner.validate(sqlNode);
                    node = planner.rel(validatedSqlNode).project();
                } catch (SqlParseException e) {
                    numFailedQueries += 1;
                    System.out.println(e);
                    System.out.println("failed to parse: " + query);
                    continue;
                    //System.exit(-1);
                } catch (ValidationException e) {
                    numFailedQueries += 1;
                    System.out.println(e);
                    System.out.println("failed to validate: " + query);
                    //throw e;
                    continue;
                } catch (Exception e) {
                    numFailedQueries += 1;
                    System.out.println(e);
                    System.out.println("failed in getting Relnode from  " + query);
                    continue;
                    //System.exit(-1);
                }
                DbInfo.setCurrentQueryVisibleFeatures(node);
                // testing if features were set correctly
                ImmutableBitSet bs = DbInfo.getCurrentQueryVisibleFeatures();
                //System.out.println("DQ features are: " + bs);

                RelMetadataQuery mq = RelMetadataQuery.instance();
                RelOptCost unoptCost = mq.getCumulativeCost(node);
                //System.out.println("unoptimized toString is: " + RelOptUtil.toString(node));
                System.out.println("unoptimized cost is: " + unoptCost);
                //System.out.println(RelOptUtil.dumpPlan("unoptimized plan:", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
                /// very important to do the replace EnumerableConvention thing
                RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
                try {
                    // Note: executing the unoptimized node here results in a
                    // planning error when trying to optimize it later.
                    // executeNode(node);

                    // FIXME: check if this might actually be working now.
                    tryHepPlanner(node, traitSet, mq);

                    // using the default volcano planner.
                    //long start = System.currentTimeMillis();
                    //RelNode optimizedNode = planner.transform(0, traitSet,
                            //node);
                    //System.out.println("printing optimized node info");
                    //System.out.println("optimized toString is: " +
                            //RelOptUtil.toString(optimizedNode));
                    //System.out.println(RelOptUtil.dumpPlan("optimized plan:", optimizedNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
                    //System.out.println("volcano optimized cost is: " + mq.getCumulativeCost(optimizedNode));
                    //System.out.println("planning time: " +
                                //(System.currentTimeMillis()- start));

                    //executeNode(optimizedNode);

                } catch (Exception e) {
                    numFailedQueries += 1;
                    System.out.println(e);
                    System.out.println(e.getStackTrace());
                    throw e;
                    //continue;
                }
                //System.out.println("SUCCESSFULLY GOT THE QUERY!!");
                numSuccessfulQueries += 1;
            }
        }
        System.out.println("numSuccessfulQueries = " + numSuccessfulQueries);
        System.out.println("numFailedQueries = " + numFailedQueries);
    }

    private Frameworks.ConfigBuilder getDefaultFrameworkBuilder() throws
        SQLException {
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

        return configBuilder;
    }

    private void tryHepPlanner(RelNode node, RelTraitSet traitSet, RelMetadataQuery mq) {
        // testing out the volcano program builder
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

        System.out.println("hep optimized toString is: " +
            RelOptUtil.toString(hepTransform));
        System.out.println("hep optimized cost is: " + mq.getCumulativeCost(hepTransform));
        //System.out.println("executing hep optimized node...");
        //executeNode(hepTransform);
    }

    private void executeNode(RelNode node) {
        try {
            RelRunner runner = conn.unwrap(RelRunner.class);
            PreparedStatement ps = runner.prepare(node);
            System.out.println("executing node");
            long start = System.currentTimeMillis();
            ResultSet res = ps.executeQuery();
            long end = System.currentTimeMillis();
            long total = end - start;
            System.out.println("execution time: " + total);
            int curLine = 0;
						while (res.next()) {
              System.out.println(res.getString(1));
              curLine += 1;
              if (curLine >= 10) {
                break;
              }
            }

        } catch (SQLException e) {
            System.out.println("caught exeception while executing query");
            System.out.println(e);
            e.printStackTrace();
        }
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
}
