import java.util.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.*;

// FIXME: copied over from QueryOptExp
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
import org.apache.calcite.rel.type.*;
import org.apache.calcite.util.ImmutableBitSet;
import java.util.concurrent.TimeUnit;

public class MyUtils {

  public static String getTableName(RelNode rel) {
    if (rel == null) {
      return null;
    }
    if (rel instanceof RelSubset) {
      RelSubset s = (RelSubset) rel;
      return getTableName(s.getOriginal());
    } else if (rel instanceof Filter) {
      return getTableName(rel.getInput(0));
    } else if (rel instanceof HepRelVertex) {
      return getTableName(((HepRelVertex) rel).getCurrentRel());
    } else if (rel instanceof TableScan) {
      List<String> names = rel.getTable().getQualifiedName();
      if (names != null) {
        //System.out.println("table name was: " + names.get(1));
        // TODO: is the more general version ever needed?
        //String tableName = "";
        //for (String s : names) {
          //tableName += s "-";
        //}
        //return tableName;
        return names.get(1);
      }
    }
    return null;
  }

  /* just a helper utility function that traverses the RelNode tree.
   */
  public static void printInfo(RelNode node) {
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

  /* FIXME: make this more flexible. Need to close ResultSet's so we just send
   * back the hash of top n rows if asked for.
   */
  public static class ExecutionResult {
    public Integer resultHashCode = -1;
    public Long runtime = 0L;
    public Double trueCardinality = -1.00;
  }

  /* @node: node to execute.
   * @ret: ExecutionResult: ResultSet, ExecutionTime
   */
  public static ExecutionResult executeNode(RelNode node, boolean getTrueCardinality)
  {
    QueryOptExperiment.Params params = QueryOptExperiment.getParams();
		if (params.clearCache) {
      //String cmd = "sudo sh -c \"echo 3 > /proc/sys/vm/drop_caches\"";
      System.out.println("going to run test.sh!");
      //String cmd = "sudo sh -c -S /home/pari/query-optimizer/test.sh";
      String cmd = "/home/pari/query-optimizer/test.sh";
      //String cmd = "sh -c ls";
      try {

				ProcessBuilder processBuilder = new ProcessBuilder(cmd);
				//Sets the source and destination for subprocess standard I/O to be the same as those of the current Java process.
				processBuilder.inheritIO();
				Process process = processBuilder.start();
        TimeUnit.SECONDS.sleep(2);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        out.write("1234\n");
        //out.flush();

				int exitValue = process.waitFor();
				if (exitValue != 0) {
						// check for errors
						new BufferedInputStream(process.getErrorStream());
						throw new RuntimeException("execution of script failed!");
				}

        //Process cmdProc = Runtime.getRuntime().exec(cmd);
				//BufferedReader stdoutReader = new BufferedReader(
								 //new InputStreamReader(cmdProc.getInputStream()));
				//String line;
				//while ((line = stdoutReader.readLine()) != null) {
          //System.out.println(line);
				//}

				//BufferedReader stderrReader = new BufferedReader(
								 //new InputStreamReader(cmdProc.getErrorStream()));
				////String line;
				//while ((line = stderrReader.readLine()) != null) {
          //System.out.println(line);
				//}

				//cmdProc.waitFor();
        //System.out.println("clearing cache succeeded. Exit code: " + cmdProc.exitValue());
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        //System.out.println("clearing cache failed!");
        System.exit(-1);
      }
    }

    /// FIXME: clean up all the resources at the end
    ResultSet rs = null;
    PreparedStatement ps = null;
    Integer resultHash = -1;
    Long runtime = -1L;
    CalciteConnection curConn = null;

    //Class.forName("org.postgresql.Driver");
    //org.postgresql.Driver.setLogLevel(org.postgresql.Driver.DEBUG);

    try {
      curConn = (CalciteConnection) DriverManager.getConnection(params.dbUrl);
      // FIXME: or do this? Will need to get conn from QueryOptExperiment
      //curConn = conn;
      //System.out.println("in execute node!");
      //System.out.println("nodeToString:\n " + RelOptUtil.toString(node));
      curConn.setAutoCommit(true);
      RelRunner runner = curConn.unwrap(RelRunner.class);
      System.out.println("after curConn.unwrap!");
      ps = runner.prepare(node);
      //ps.setQueryTimeout(100);
      //System.out.println(ps);
      ps.setQueryTimeout(1000000);
      long start = System.currentTimeMillis();
      System.out.println("executing node");
      rs = ps.executeQuery();
      long end = System.currentTimeMillis();
      runtime = end - start;
      System.out.println("execution time: " + runtime);
    } catch (Exception e) {
      System.out.println("caught exception while executing query");
      System.out.println(e);
      e.printStackTrace();
      try {
        curConn.close();
        ps.close();
      } catch (Exception e2) {
        // ignore it?
        System.out.println("caught second exception while executing query");
        System.out.println(e2);
        e2.printStackTrace();
      }
      return null;
      //System.exit(-1);
    }
    System.out.println("execution DID NOT crash");
    ExecutionResult execResult;
    // this can be an expensive operation, so only do it if really needed.
    if (params.verifyResults || getTrueCardinality) {
      execResult = getResultSetHash(rs);
      execResult.runtime = runtime;
    } else {
      // default values
      execResult = new ExecutionResult();
      execResult.runtime = runtime;
    }
    /* clean up the remaining used resources */

    try {
      //TimeUnit.SECONDS.sleep(2);
      curConn.close();
      ps.close();
      rs.close();
    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
      // no good way to handle this (?)
      System.exit(-1);
    }
    // clear cache
    return execResult;
  }

  public static ExecutionResult getResultSetHash(ResultSet res)
  {
    String combinedResults = "";
    Double curLine = 0.00;
    try {
      ResultSetMetaData rmd = res.getMetaData();
      int num_columns = rmd.getColumnCount();
      System.out.println("num columns " + num_columns);
      while (res.next()) {
        // FIXME: do we want to go over all columns or not?
        for (int i = 1; i < num_columns+1; i++) {
          combinedResults += res.getString(i);
        }
        curLine += 1.00;
      }
    } catch (Exception e) {
      // ignore for now.
    }
    ExecutionResult execResult = new ExecutionResult();
    execResult.resultHashCode = combinedResults.hashCode();
    execResult.trueCardinality = curLine;
    return execResult;
  }
}
