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
import org.apache.calcite.plan.volcano.*;
import java.util.regex.*;
//import org.apache.calcite.plan.hep.HepProgramBuilder;

import org.apache.calcite.config.Lex;

//import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.*;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.commons.io.FileUtils;

// experimental
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;

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
                Program program =
                    Programs.ofRules(NoneConverterRule.INSTANCE,
                            ProjectCalcMergeRule.INSTANCE);
                List<Program> rules = new ArrayList<Program>();
                rules.add(program);
                bld.programs(rules);
            } else if (t == PLANNER_TYPE.LOpt) {

            } else if (t == PLANNER_TYPE.RANDOM) {

            } else {
                assert false : "haven't implemented any other planner types yet";
            }
            FrameworkConfig cfg = bld.build();
            System.out.println("cost Factory: " + cfg.getCostFactory());
            Planner planner = Frameworks.getPlanner(cfg);
            planners.add(planner);
        }

        // load in the sql queries dataset
        if (queries == QUERIES_DATASET.JOB) {
            // should be in the directory join-order-benchmark
            File dir = new File(DATASET_PATH);
            File[] listOfFiles = dir.listFiles();
            for (File f : listOfFiles) {
                //if (f.getName().contains(".sql")) {
                if (f.getName().contains("10a.sql")) {
                    String sql;
                    try {
                        sql = FileUtils.readFileToString(f);
                    } catch (Exception e) {
                        System.out.println("could not read file " + f.getName());
                        continue;
                    };
                    // FIXME: parse the sql to avoid calcite errors.
                    //String escapedSql = queryRewriteForCalcite(sql);
                    String escapedSql = sql.replace(";", "");
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

        configBuilder.costFactory(TestCost.FACTORY);

        return configBuilder;
    }

    /* Runs all the planners we have on all the given allSqlQueries, and collects
     * statistics about each run.
     */
    public void run(ArrayList<String> queries) throws Exception {
        int numSuccessfulQueries = 0;
        int numFailedQueries = 0;
        for (String query : queries) {
            System.out.println("numSuccessfulQueries = " + numSuccessfulQueries);
            System.out.println("numFailedQueries = " + numFailedQueries);
            //System.out.println(query);
            //query = "select * from name";
            //query = "select * from name join aka_name on name.name = aka_name.name";
            //if (numSuccessfulQueries == 1 || numFailedQueries == 1) break;
            if (numSuccessfulQueries == 10) break;

			for (Planner planner : planners) {
                planner.close();
                planner.reset();
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
                System.out.println(RelOptUtil.dumpPlan("unoptimized plan:", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
				/// very important to do the replace EnumerableConvention thing
				//RelTraitSet traitSet = planner.getEmptyTraitSet().replace(EnumerableConvention.INSTANCE);
                /// desperate try:
				RelTraitSet traitSet = planner.getEmptyTraitSet().replace(Convention.NONE);
                try {
                    RelNode transform = planner.transform(0, traitSet, node);
                    System.out.println(RelOptUtil.dumpPlan("optimized plan:", transform, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
                    System.out.println("optimized cost is: " + mq.getNonCumulativeCost(transform));
                    System.out.println("transform node convention is: " + transform.getConvention());

                    // Testing out the HepPlanner.
                    tryHepPlanner(node, traitSet, mq);
                    //tryVolcanoPlanner(node, traitSet, mq);

					//RelRunner runner = conn.unwrap(RelRunner.class);
                    ////PreparedStatement ps = runner.prepare(transform);
                    //System.out.println("executing unoptimized node");
					//PreparedStatement ps = runner.prepare(node);
					////// testing simpler method of preparing statement for execution
					//System.out.println("prepare statement done!");
                    //long tStart = System.currentTimeMillis();
					//ResultSet res2 = ps.executeQuery();
                    //long tend = System.currentTimeMillis();
                    //long total = tend - tStart;
                    //System.out.println("execution time: " + total);

                } catch (Exception e) {
                    throw e;
                    //numFailedQueries += 1;
                    //System.out.println(e);
                    //System.out.println(e.getStackTrace());
                    //continue;
                }
                System.out.println("SUCCESSFULLY GOT THE QUERY!!");
                numSuccessfulQueries += 1;
                //planner.close();
                //planner.reset();
            }
        }
        System.out.println("numSuccessfulQueries = " + numSuccessfulQueries);
        System.out.println("numFailedQueries = " + numFailedQueries);
    }

    // FIXME: need to ensure this works fine in all cases.
    private String queryRewriteForCalcite(String query) {
        class Join {
            // these should correspond to full table names and not aliases
            public String left;
            public String right;
            public String cond;

            public Join(String left, String right, String cond) {
                this.left = left;
                this.right = right;
                this.cond = cond;
            }
        }

        HashMap<String, String> tableAliases = new HashMap<String, String>();
        ArrayList<String> whereClauses = new ArrayList<String>();
        ArrayList<String> selectStatements = new ArrayList<String>();
        // key will be the two table names concatenated. Value will be
        // potentially more than one conditions (joined by AND)
        //HashMap<String, String> joinClauses = new HashMap<String, String>();
        ArrayList<Join> joins = new ArrayList<Join>();

        boolean FROM = false;
        boolean WHERE = false;
    	BufferedReader bufReader = new BufferedReader(new StringReader(query));
		String line = null;
        try {
            while( (line = bufReader.readLine()) != null ) {
                // wherever we encounter these because calcite doesn't like it
                line = line.replace(";", "");
                if (line.equals("")) continue;
                if (line.contains("FROM")) {
                    FROM = true;
                } else if (line.contains("WHERE")) {
                    WHERE = true;
                    FROM = false;
                }

                if (FROM) {
                    line = line.replace("FROM", "");
                    String[] data = line.split("[AS]");
                    tableAliases.put(data[2].trim().replace(",",""), data[0].trim());
                } else if (WHERE) {
                    // need to check if two of the keys are present in this
                    // statement.
                    //int numKeysPresent = 0;
                    ArrayList<String> keysPresent = new ArrayList<String>();
                    for (String k : tableAliases.keySet()) {
                        // FIXME: better condition
                        if (exactContains(line, k + ".")) {
                            keysPresent.add(k);
                        }
                    }
                    int numKeysPresent = keysPresent.size();
                    if (numKeysPresent > 2 || numKeysPresent == 0) {
                        System.err.println("only 1 or two tables should have been there in \n" + line);
                        continue;
                        //System.exit(-1);
                    }

                    if (numKeysPresent == 1) {
                        String key1 = keysPresent.get(0);
                        // replace the alias with the fully qualified name
                        // poor man's regex using "." to avoid unwanted
                        // replacements since table names are always followed
                        // by that
                        line = line.replace(key1 + ".", tableAliases.get(key1) + ".");
                        line = line.replace("AND", "");
                        whereClauses.add(line);
                    } else if (numKeysPresent == 2) {
                        // we know this must be a join, so let us parse this
                        // statement and add it to our list of joins.

                        String key1 = keysPresent.get(0);
                        String key2 = keysPresent.get(1);
                        String table1 = tableAliases.get(key1);
                        String table2 = tableAliases.get(key2);

                        // the line represents the condition, but we want to
                        // replace the aliases with fully qualified table names
                        line = line.replace(key1 + ".", table1 + ".");
                        line = line.replace(key2 + ".", table2 + ".");
                        line = line.replace("AND", "");

                        // Note: we should ONLY add this to the list of joins
                        // if at least one of these tables haven't already been
                        // added. If they have both been added before, then we
                        // should add it to whereClauses.
                        boolean table1Added = false;
                        boolean table2Added = false;
                        for (int i = 0; i < joins.size(); i++) {
                            Join iJoin = joins.get(i);
                            if (iJoin.left.equals(table1) ||
                                    iJoin.right.equals(table1)) {
                                table1Added = true;
                            }
                            if (iJoin.left.equals(table2) ||
                                    iJoin.right.equals(table2)) {
                                table2Added = true;
                            }
                        }
                        if (table1Added && table2Added) {
                            whereClauses.add(line);
                        } else {
                            Join curJoin = new Join(table1, table2, line);
                            joins.add(curJoin);
                        }
                    }
                } else {
                    /* just write these lines as it is */
                    selectStatements.add(line);
                }
            }
        } catch (Exception e) {
            System.exit(-1);
        }
        String newQuery = "";
        for (int i = 0; i < selectStatements.size(); i++) {
            String s1 = selectStatements.get(i);
            String selStatement = s1;
            String[] spl = s1.split("\\W");
            for (String s2 : spl) {
                // add all these as is back to newQuery except the table
                // aliases
                //System.out.println("s2: " + s2);
                String tableName = tableAliases.get(s2);
                if (tableName != null) {
                    //System.out.println("not null!");
                    selStatement = selStatement.replace(s2 + ".", tableName + ".");
                }
            }
            selStatement = selStatement.split("AS")[0];
            if (i != selectStatements.size()-1) selStatement += ",";
            newQuery += selStatement;
        }
        newQuery += " FROM \n";

        //int numClauses = 0;
        ArrayList<String> usedTables = new ArrayList<String>();
        ArrayList<Join> unusedJoins = new ArrayList<Join>();
        // now we need to make two passes over the joins - because we need to
        // be careful of the order in which we add each successive join. After
        // the first one, it must be that one of the previous joins must be
        // present in the joins used so far.
        String joinClause = "";
        for (int i = 0; i < joins.size(); i++) {
            Join curJoin = joins.get(i);
            String left, right;
            //String right;
            if (i == 0) {
                left = curJoin.left;
                right = curJoin.right;
                usedTables.add(left);
            } else {
                left = joinClause;
                if (usedTables.contains(curJoin.left)) {
                    // the way we added elements to joins, we know that one of the joins must be a new element. So if left has already been added in the joins, then right must be new.
                    right = curJoin.right;
                } else if (usedTables.contains(curJoin.right)) {
                    right = curJoin.left;
                } else {
                    // if both have not been added so far, then just ignore
                    // them until next loop.
                    unusedJoins.add(curJoin);
                    continue;
                }
            }
            joinClause = left.trim() + "\nJOIN " + right.trim() + " ON " + curJoin.cond;
            usedTables.add(right);
        }

        System.out.println("join clause after first loop is so far is: " + joinClause);
        for (int i = 0; i < unusedJoins.size(); i++) {
            Join curJoin = unusedJoins.get(i);
            String left = joinClause;
            String right = "";
            if (usedTables.contains(curJoin.left)) {
                // the way we added elements to joins, we know that both of
                // them must not be added. so only left must have bene
                // added.
                right = curJoin.left;
            } else if (usedTables.contains(curJoin.right)) {
                right = curJoin.right;
            } else {
                System.err.println("should not have happened!");
                return newQuery;
                //System.exit(-1);
            }
            joinClause = left.trim() + "\nJOIN " + right.trim() + " ON " + curJoin.cond;
            usedTables.add(right);
        }

        newQuery += joinClause;

        newQuery += "\n WHERE \n";
        for (int i = 0; i < whereClauses.size(); i++) {
            String s = whereClauses.get(i);
            s = s.replace("WHERE", "");
            if (i != 0) s = "AND " + s;
            newQuery += s;
            //if (i+1 != whereClauses.size()) {
                //newQuery += ",";
            //}
            newQuery += "\n";
        }
        System.out.println("newQuery is: \n" + newQuery);
        return newQuery;
    }

	private static boolean exactContains(String source, String subItem){
         String pattern = "\\b"+subItem+"\\b";
         Pattern p=Pattern.compile(pattern);
         Matcher m=p.matcher(source);
         return m.find();
    }

    private void tryVolcanoPlanner(RelNode node, RelTraitSet traitSet,
            RelMetadataQuery mq) {

        VolcanoPlanner planner = new VolcanoPlanner();
        RelNode newNode = node.copy(traitSet, node.getInputs());
        planner.setRoot(newNode);

        planner.addRule(FilterMergeRule.INSTANCE);
        planner.addRule(LoptOptimizeJoinRule.INSTANCE);

        planner.changeTraits(node, traitSet);
        System.out.println("added all rules to HepPlanner");

        RelNode hepTransform = planner.findBestExp();

        System.out.println(RelOptUtil.dumpPlan("optimized hep plan:", hepTransform, SqlExplainFormat.TEXT, SqlExplainLevel.NO_ATTRIBUTES));
        System.out.println("optimized cost is: " + mq.getNonCumulativeCost(hepTransform));
    }

    // Somehow this seems to just return the node as is despite having added
    // rules...
	private void tryHepPlanner(RelNode node, RelTraitSet traitSet, RelMetadataQuery mq) {
        // testing out the volcano program builder
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        // addClass does not seem to help
        hepProgramBuilder.addRuleClass(FilterMergeRule.class);
        hepProgramBuilder.addRuleClass(LoptOptimizeJoinRule.class);

        hepProgramBuilder.addRuleInstance(FilterMergeRule.INSTANCE);
        hepProgramBuilder.addRuleInstance(LoptOptimizeJoinRule.INSTANCE);


        HepPlanner hepPlanner = new HepPlanner(hepProgramBuilder.build());
        //for (RelOptRule rule : Programs.RULE_SET) {
            //hepPlanner.addRule(rule);
        //}

        hepPlanner.setRoot(node);
        hepPlanner.addRule(FilterMergeRule.INSTANCE);
        hepPlanner.addRule(LoptOptimizeJoinRule.INSTANCE);

        hepPlanner.changeTraits(node, traitSet);
        System.out.println("added all rules to HepPlanner");
        // register metadata provider
        final RelMetadataProvider provider = node.getCluster().getMetadataProvider();

        // Register RelMetadataProvider with HepPlanner.
        final ArrayList<RelMetadataProvider> list = new ArrayList<RelMetadataProvider>();
        list.add(provider);
        hepPlanner.registerMetadataProviders(list);
        System.out.println("registered metadata provider with hep planner");

        final RelMetadataProvider cachingMetaDataProvider = new CachingRelMetadataProvider(ChainedRelMetadataProvider.of(list), hepPlanner);
        node.accept(new MetaDataProviderModifier(cachingMetaDataProvider));
        RelNode hepTransform = hepPlanner.findBestExp();

        System.out.println(RelOptUtil.dumpPlan("optimized hep plan:", hepTransform, SqlExplainFormat.TEXT, SqlExplainLevel.NO_ATTRIBUTES));
        System.out.println("optimized cost is: " + mq.getNonCumulativeCost(hepTransform));

    }

    /// PN: this from samza. Not sure what it does either.
    // TODO: This is from Drill. Not sure what it does.
    public static class MetaDataProviderModifier extends RelShuttleImpl {
        private final RelMetadataProvider metadataProvider;

        public MetaDataProviderModifier(RelMetadataProvider metadataProvider) {
          this.metadataProvider = metadataProvider;
        }

        @Override
        public RelNode visit(TableScan scan) {
          scan.getCluster().setMetadataProvider(metadataProvider);
          return super.visit(scan);
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
          scan.getCluster().setMetadataProvider(metadataProvider);
          return super.visit(scan);
        }

        @Override
        public RelNode visit(LogicalValues values) {
          values.getCluster().setMetadataProvider(metadataProvider);
          return super.visit(values);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
          child.accept(this);
          parent.getCluster().setMetadataProvider(metadataProvider);
          return parent;
        }
    }

}
