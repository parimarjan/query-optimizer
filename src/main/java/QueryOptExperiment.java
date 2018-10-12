import java.sql.*;
import java.util.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.tools.*;
import java.io.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.plan.hep.*;
//import org.apache.calcite.plan.hep.HepProgramBuilder;

import org.apache.calcite.config.Lex;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.commons.io.FileUtils;

// experimental
import org.apache.calcite.adapter.enumerable.*;

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
        DQ;
    }
    private ArrayList<Planner> planners;

    // FIXME: combine dataset / with path.
	public enum QUERIES_DATASET
    {
        JOB;
    }
    private String DATASET_PATH = "./join-order-benchmark/";

    // all the queries we have. These should be parsed properly to avoid
    // calcite's troubles with " etc.
    // TODO: explain parsing.
    public ArrayList<String> allSqlQueries;

	/*
    *************************************************************************
    *************************************************************************
                                    Methods
    *************************************************************************
    *************************************************************************
    */


	/* @dbUrl
     */
	public QueryOptExperiment(String dbUrl, List<PLANNER_TYPE> plannerTypes, QUERIES_DATASET queries) throws SQLException {

		conn = (CalciteConnection) DriverManager.getConnection(dbUrl);
        planners = new ArrayList<Planner>();
        allSqlQueries = new ArrayList<String>();

        // Initialize all the planners we should need
        for (PLANNER_TYPE t  : plannerTypes) {
            Frameworks.ConfigBuilder bld = getDefaultFrameworkBuilder();
            if (t == PLANNER_TYPE.EXHAUSTIVE) {
                // FIXME: Need to select only the exhaustive search rules and
                // modify genJoinRule appropriately to take in list.
                // crashes if we add no rules here.
                //bld.programs(MyJoinUtils.genJoinRule(Programs.RULE_SET, 2));
                //Program program = Programs.ofRules(FilterMergeRule.INSTANCE,
                        //EnumerableRules.ENUMERABLE_FILTER_RULE,
                        //EnumerableRules.ENUMERABLE_PROJECT_RULE);
                Program program = Programs.ofRules(ProjectCalcMergeRule.INSTANCE);
                List<Program> rules = new ArrayList<Program>();
                rules.add(program);
                bld.programs(rules);
            } else if (t == PLANNER_TYPE.LOpt) {

            } else if (t == PLANNER_TYPE.RANDOM) {

            } else {
                assert false : "haven't implemented any other planner types yet";
            }
            Planner planner = Frameworks.getPlanner(bld.build());
            //planner.addRule(FilterMergeRule.INSTANCE);
            planners.add(planner);
        }

        // load in the sql queries dataset
        if (queries == QUERIES_DATASET.JOB) {
            // should be in the directory join-order-benchmark
            File dir = new File(DATASET_PATH);
            File[] listOfFiles = dir.listFiles();
            for (File f : listOfFiles) {
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
                }
            }
        } else {
            assert false : "do not have any other dataset";
        }
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
        parserBuilder.setQuotedCasing(Lex.JAVA.quotedCasing)
                     .setUnquotedCasing(Lex.JAVA.unquotedCasing)
                     .setQuoting(Lex.JAVA.quoting)
                     .setCaseSensitive(Lex.JAVA.caseSensitive);

        configBuilder.parserConfig(parserBuilder.build());

		// FIXME: experimental stuff
        final List<RelTraitDef> traitDefs = new ArrayList<RelTraitDef>();
        traitDefs.add(ConventionTraitDef.INSTANCE);
        //traitDefs.add(EnumerableConvention.INSTANCE);
        traitDefs.add(RelCollationTraitDef.INSTANCE);
        configBuilder.traitDefs(traitDefs);
        configBuilder.context(Contexts.EMPTY_CONTEXT);

        return configBuilder;
    }

    /* Runs all the planners we have on all the given allSqlQueries, and collects
     * statistics about each run.
     */
    public void run(ArrayList<String> queries) throws Exception {
        int numSuccessfulQueries = 0;
        int numFailedQueries = 0;
        for (String query : queries) {
            //System.out.println(query);
            //query = "select * from name";
            //query = "select * from name join aka_name on name.name = aka_name.name";
            query = "select * from cast_info join role_type on cast_info.role_id = role_type.id join person_info on person_info.id = cast_info.id";
            if (numSuccessfulQueries == 1) break;
			for (Planner planner : planners) {
                RelNode node = null;
                try {
                    SqlNode sqlNode = planner.parse(query);
                    System.out.println(sqlNode);
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
                    continue;
                    //System.exit(-1);
                } catch (Exception e) {
                    numFailedQueries += 1;
                    System.out.println(e);
                    System.out.println("failed in getting Relnode from  " + query);
                    continue;
                    //System.exit(-1);
                }

                RelMetadataQuery mq = RelMetadataQuery.instance();
				RelOptCost unoptCost = mq.getNonCumulativeCost(node);
				System.out.println("non optimized cost is: " + unoptCost);
                System.out.println(RelOptUtil.dumpPlan("unoptimized plan:", node, SqlExplainFormat.TEXT, SqlExplainLevel.NO_ATTRIBUTES));
				/// very important to do the replace EnumerableConvention thing
				RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
                try {
                    RelNode transform = planner.transform(0, traitSet, node);
                    HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
                    HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build());
                    hepPlanner.setRoot(node);
                    //hepPlanner.addRule(FilterMergeRule.INSTANCE);
                    //hepPlanner.addRule(LoptOptimizeJoinRule.INSTANCE);
                    for (RelOptRule rule : Programs.RULE_SET) {
                        hepPlanner.addRule(rule);
                    }
                    hepPlanner.changeTraits(node, traitSet);
                    RelNode hepTransform = hepPlanner.findBestExp();

                    System.out.println(RelOptUtil.dumpPlan("optimized hep plan:", hepTransform, SqlExplainFormat.TEXT, SqlExplainLevel.NO_ATTRIBUTES));
                    System.out.println("optimized cost is: " + mq.getNonCumulativeCost(hepTransform));


                    System.out.println(RelOptUtil.dumpPlan("optimized plan:", transform, SqlExplainFormat.TEXT, SqlExplainLevel.NO_ATTRIBUTES));
                    System.out.println("optimized cost is: " + mq.getNonCumulativeCost(transform));
                } catch (Exception e) {
                    numFailedQueries += 1;
                    System.out.println(e);
                    System.out.println(e.getStackTrace());
                    continue;
                }
                //System.out.println("SUCCESSFULLY GOT THE QUERY!!");
                numSuccessfulQueries += 1;
                planner.close();
                planner.reset();
            }
        }
        System.out.println("numSuccessfulQueries = " + numSuccessfulQueries);
        System.out.println("numFailedQueries = " + numFailedQueries);
    }

    // FIXME: need to ensure this works fine in all cases.
    private String queryRewriteForCalcite(String query) {
        //System.out.println("original query: " + query);
        String newQuery = query.replace(";", "");
        //System.out.println("rewritten query: " + newQuery);
        return newQuery;
    }
}
