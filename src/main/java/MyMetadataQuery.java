// FIXME: too many imports?
//import org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.RelOptUtil;

import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.adapter.jdbc.*;
import org.apache.calcite.rel.logical.*;
import java.util.*;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.*;
//import com.google.gson.Gson;
//import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import java.util.concurrent.ThreadLocalRandom;

//import org.apache.calcite.rel.rel2sql;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
//import org.apache.calcite.rel.RelToSqlConverter;
//import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Result;
import org.apache.calcite.sql.SqlNode;
import java.util.Random;

public class MyMetadataQuery extends RelMetadataQuery {
  private class JsonCardinalities
  {
    Map<String, Map<String, Double>> cardinalities;
  }

  // in terms of number of rows. This is used for calculating the cost in a
  // non-linear model.
  private final double MEMORY_LIMIT = Math.pow(10, 6);
  // TODO: support options: CM2, rowCount, MM
  private final String COST_MODEL_NAME;

  // FIXME: temporary solution. make this general purpose.
  private final String BASE_CARDINALITIES_FILE_NAME = "cardinalities.ser";
  public HashMap<String, HashMap<String, Double>> trueBaseCardinalities;
  // private final String CARDINALITIES_FILE = "test.json";
  //public HashMap<String, HashMap<String, Long>> cards;

  /**
   * Returns an instance of RelMetadataQuery. It ensures that cycles do not
   * occur while computing metadata.
   */
  public static MyMetadataQuery instance() {
    return new MyMetadataQuery(THREAD_PROVIDERS.get(), EMPTY);
  }

	protected MyMetadataQuery(JaninoRelMetadataProvider metadataProvider,
      RelMetadataQuery prototype) {
		super(metadataProvider, prototype);
    this.COST_MODEL_NAME = QueryOptExperiment.getCostModelName();
    trueBaseCardinalities = (HashMap) loadCardinalities();
    if (trueBaseCardinalities == null) {
      trueBaseCardinalities = new HashMap<String, HashMap<String, Double>>();
    }
  }

  @Override
  public Double getRowCount(RelNode rel) {

    QueryOptExperiment.Params params = QueryOptExperiment.getParams();
    Query query = QueryOptExperiment.getCurrentQuery();

    // in this case, the provided cardinality file should have entries for
    // each of the needed queries.
    // TODO: explain the format better.
    ArrayList<String> tableNames = MyUtils.getAllTableNames(rel);
    Double rowCount = null;
    if (params.cardinalities != null) {
      java.util.Collections.sort(tableNames);
      String tableKey = "";
      for (String tN : tableNames) {
        tableKey += " " + tN;
      }
      if (!tableKey.contains("null")) {
        // FIXME: fix the join-order-benchmark case
        //String fileName = "join-order-benchmark/" + query.queryName;
        String key = query.queryName;
        HashMap<String, Long> qCards = params.cardinalities.get(key);
        if (qCards == null) {
          System.out.println("qCards is null for key: " + key);
          System.exit(-1);
        } else {
          Long rowCountLong = qCards.get(tableKey);
          //System.out.println("found rowCount from file!: " + rowCount);
          if (rowCountLong != null) {
            rowCount = rowCountLong.doubleValue();
            return rowCount;
          }
          //System.out.println("row count was null!");
          System.out.println("fileName: " + query.queryName);
          System.out.println("tableKey: " + tableKey);
          //return 10000000.00;
          System.exit(-1);
        }
      } else {
        // these seem to happen mostly for aggregate nodes etc.
        // System.out.println("tableKey had null: " + tableKey);
        // System.out.println(rel);
      }
    }
    if (rowCount == null) {
      // Default: use true cardinalities for the base tables, and calcite's
      // default handling for all the joins (some sort of very simple Selinger
      // model...)
      String sqlQuery = query.sql;
      HashMap<String, Double> curQueryMap = trueBaseCardinalities.get(sqlQuery);
      if (curQueryMap == null) {
        //System.out.println("case 1");
        rowCount = super.getRowCount(rel);
      }
      if (rel instanceof Filter || rel instanceof TableScan) {
        String tableName = MyUtils.getTableName(rel);
        if (tableName == null) {
          rowCount = super.getRowCount(rel);
        } else {
          rowCount = curQueryMap.get(tableName);
        }
        if (rowCount == null) {
          //System.out.println("case 2");
          rowCount = super.getRowCount(rel);
        }
      } else if (rel instanceof RelSubset) {
        // this seems like it should need special handling, but it probably wraps
        // around either Filter / TableScan, so it will be handled when this
        // function is called again.
        rowCount = super.getRowCount(rel);
      }
      if (rowCount == null) {
        //System.out.println("case 5");
        rowCount = super.getRowCount(rel);
      }
    }
    //System.out.println("final rowCount returned: " + rowCount);
    return rowCount;
  }

  private RelOptCost getHashJoinCost(Join rel) {
    RelOptCost curCost = new MyCost(0.0, 0.0, 0.0);
    RelNode left = ((Join) rel).getLeft();
    RelNode right = ((Join) rel).getRight();
    //RelOptCost leftCost = getCumulativeCost(left);
    //RelOptCost rightCost = getCumulativeCost(right);
    //double leftRows = ((MyCost) leftCost).cost;
    //double rightRows = ((MyCost) rightCost).cost;
    double leftRows = getRowCount(left);
    double rightRows = getRowCount(right);
    ((MyCost) curCost).cost = leftRows + rightRows;
    // In MM cost model, assume pipelining, so current node's rows would be
    // used to calculate the cost of downstream nodes.
    //double curRows = getRowCount(rel);
    //((MyCost) curCost).cost = leftRows + rightRows + curRows;
    return curCost;
  }

  private RelOptCost getIndexNestedLoopJoinCost(Join rel) {
    // Need to ensure right is a base relation, and has an index built on it.
    MyCost curCost = new MyCost(0.0, 0.0, 0.0);
    //RelNode right = rel.getRight();
    RelNode left = rel.getLeft();
    double leftRows = getRowCount(left);
    curCost.cost = 2.00*leftRows;
    return curCost;
  }

	@Override
  public RelOptCost getCumulativeCost(RelNode rel) {
    return super.getCumulativeCost(rel);
  }

	@Override
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    RelOptCost origCost = super.getNonCumulativeCost(rel);
    QueryOptExperiment.Params params = QueryOptExperiment.getParams();
    if (COST_MODEL_NAME.equals("MM")) {
      RelOptCost curCost = new MyCost(0.0, 0.0, 0.0);
      if (rel instanceof Join) {
        RelOptCost hashJoinCost = getHashJoinCost((Join) rel);
        ArrayList<String> rightTables = MyUtils.getAllTableNames(((Join)rel).getRight());
        if (rightTables.size() > 1 || !params.useIndexNestedLJ) {
          return hashJoinCost;
        }

        RelOptCost indexLoopCost = getIndexNestedLoopJoinCost((Join) rel);
        if (indexLoopCost.isLt(hashJoinCost)) {
          return indexLoopCost;
        } else {
          return hashJoinCost;
        }
      } else if (rel instanceof Filter || rel instanceof TableScan) {
        double rows = getRowCount(rel);
        ((MyCost) curCost).cost = params.scanCostFactor * rows;
        return curCost;
      } else {
        // just return 0 cost for the rest.
        return curCost;
      }
    } else if (rel instanceof Join && COST_MODEL_NAME.equals("CM2")) {
      RelNode left = ((Join) rel).getLeft();
      RelNode right = ((Join) rel).getRight();
      double leftRows = getRowCount(left);
      double rightRows = getRowCount(right);
      double curRows = getRowCount(rel);
      // Now, the cost model will consider whether the leftRows and rightRows
      // fit in the memory or not.
      if ((leftRows + rightRows) < MEMORY_LIMIT) {
        return origCost;
      } else if (Math.min(leftRows, rightRows) < MEMORY_LIMIT) {
        //System.out.println("!!!case 2!!!");
        double newCost = 2*(leftRows + rightRows) + curRows;
        ((MyCost) origCost).cost = newCost;
      } else {
        double newCost = rightRows + Math.ceil(rightRows / MEMORY_LIMIT) * leftRows + curRows;
        ((MyCost) origCost).cost = newCost;
      }
    }
    return origCost;
  }

  public void saveUpdatedCardinalities() {
    HashMap<String, HashMap<String, Double>> oldCosts = (HashMap) loadCardinalities();
    HashMap<String, HashMap<String, Double>> newCosts = new HashMap<String, HashMap<String, Double>>();
    if (oldCosts != null){
      // ignore this guy, file probably didn't exist.
      newCosts.putAll(oldCosts);
    }
    newCosts.putAll(trueBaseCardinalities);
    saveCardinalities(newCosts);
  }

  // FIXME: make these general purpose
  private void saveCardinalities(Serializable obj)
  {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(BASE_CARDINALITIES_FILE_NAME)
			);
			oos.writeObject(obj);
			oos.flush();
			oos.close();
		} catch (Exception e) {
			// System.out.println(e);
		}
  }

  public Serializable loadCardinalities() {
    try {
      FileInputStream fis = new FileInputStream(BASE_CARDINALITIES_FILE_NAME);
      ObjectInputStream ois = new ObjectInputStream(fis);
      HashMap<String, HashMap<String, Double>> cards = (HashMap) ois.readObject();
      ois.close();
      return cards;
    } catch (Exception e) {
      // System.out.println(e);
    }
    return null;
  }
}

