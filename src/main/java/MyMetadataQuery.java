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

public class MyMetadataQuery extends RelMetadataQuery
{

  private class JsonCardinalities
  {
    Map<String, Map<String, Double>> cardinalities;
  }

  // in terms of number of rows. This is used for calculating the cost in a
  // non-linear model.
  private final double MEMORY_LIMIT = Math.pow(10, 6);
  // TODO: support options: CM2, rowCount, MM
  private final String COST_MODEL_NAME;

  /// so can access statistics about THIS particular sql
  String queryName = null;
  public void setQueryName(String queryName) {
    this.queryName = queryName;
  }

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
  }

  @Override
  public Double getRowCount(RelNode rel)
  {
    QueryOptExperiment.Params params = QueryOptExperiment.getParams();
    if (params.cardinalities == null) {
      System.err.println("params.cardinalities need to be set to use this metadata provider");
      System.exit(-1);
    }

    String curQueryName;
    if (queryName == null) {
      System.out.println("queryName null");
      Query query = QueryOptExperiment.getCurrentQuery();
      curQueryName = query.queryName;
    } else {
      curQueryName = queryName;
    }

    // in this case, the provided cardinality file should have entries for
    // each of the needed queries.
    ArrayList<String> tableNames = MyUtils.getAllTableNamesWithFilter(rel);
    Double rowCount = null;

    java.util.Collections.sort(tableNames);
    String tableKey = "";
    tableKey += tableNames.get(0);
    for (int ti=1; ti < tableNames.size(); ti++) {
      tableKey += " " + tableNames.get(ti);
    }

    if (!tableKey.contains("null")) {
      String key = curQueryName;
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
        System.out.println("fileName: " + curQueryName);
        System.out.println("tableKey: " + tableKey);
        System.out.println("going to exit");
        System.exit(-1);
        //return 10000000000.00;
      }
    } else {
       // these seem to happen mostly for aggregate nodes etc.
       //System.out.println("tableKey had null: " + tableKey);
       //System.out.println(rel);
       return 1.00;
    }

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

  /* FIXME: this ensures that the cost model does not differentiate between AB
   * or BA, and just returns the min (cost(AB), cost(BA))
   */
  private RelOptCost getIndexNestedLoopJoinCost(Join rel) {
    // Need to ensure right is a base relation, and has an index built on it.
    MyCost curCost = new MyCost(0.0, 0.0, 0.0);
    RelNode right = rel.getRight();
    RelNode left = rel.getLeft();
    double leftRows = getRowCount(left);
    double rightRows = getRowCount(right);
    if (leftRows <= rightRows) {
      curCost.cost = 2.00*leftRows;
    } else {
      curCost.cost = 2.00*rightRows;
    }
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
        ArrayList<String> leftTables = MyUtils.getAllTableNames(((Join)rel).getLeft());

        // if both the sides have more than 1 table, then can't use index
        // nested loop join.
        if ((rightTables.size() > 1 && leftTables.size() > 1)
            || !params.useIndexNestedLJ) {
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
}

