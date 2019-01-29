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

public class MyMetadataQuery extends RelMetadataQuery {

  // in terms of number of rows. This is used for calculating the cost in a
  // non-linear model.
  private final double MEMORY_LIMIT = Math.pow(10, 6);
  // TODO: support options: CM1, CM2, CM3, RowCount
  private final String COST_MODEL_NAME;

  // FIXME: temporary solution. make this general purpose.
  private final String BASE_CARDINALITIES_FILE_NAME = "cardinalities.ser";
  public HashMap<String, HashMap<String, Double>> trueBaseCardinalities;

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
    //System.out.println("getRowCount, node is type: " + rel.getClass().getName());
    // FIXME: error checking needs to be done here!!!!
    Query query = QueryOptExperiment.getCurrentQuery();
    String sqlQuery = query.sql;
    HashMap<String, Double> curQueryMap = trueBaseCardinalities.get(sqlQuery);
    if (curQueryMap == null) return super.getRowCount(rel);
    if (rel instanceof Filter || rel instanceof TableScan) {
      String tableName = MyUtils.getTableName(rel);
      Double trueCard = curQueryMap.get(tableName);
      if (trueCard == null) {
        return super.getRowCount(rel);
      }
      String nodeString = RelOptUtil.toString(rel);
      //System.out.println(nodeString + trueCard);
      return trueCard;
    } else if (rel instanceof RelSubset) {
      // this seems like it should need special handling, but it probably wraps
      // around either Filter / TableScan, so it will be handled when this
      // function is called again.
      return super.getRowCount(rel);
    }
    return super.getRowCount(rel);
    //Double rowCount = null;
    //boolean computeTrueCard = false;
    //if (rel instanceof TableScan || rel instanceof JdbcTableScan) {
      //System.out.println("TableScan!");
      ////computeTrueCard = true;
    //} else if (rel instanceof Filter || rel instanceof LogicalFilter) {
      //// if child is TableScan, then it is Filter ( Scan ( ...)) block, so we
      //// should compute the true cardinality of the base table
      //System.out.println("child of Filter is: " + rel.getInput(0));
      //if (rel.getInput(0) instanceof TableScan || rel.getInput(0) instanceof RelSubset) {
        ////computeTrueCard = true;
      //}
    //} else if (rel instanceof RelSubset) {
      //// just compute TrueCard here as well?
      ////return getRowCount(rel.getInput(0));
      //computeTrueCard = true;
    //}
    //if (computeTrueCard) {
      //rowCount = QueryOptExperiment.getTrueCardinality(rel);
    //} else {
      //System.out.println("calling super class");
      //rowCount = super.getRowCount(rel);
    //}
    //System.out.println("returning rowCount: " + rowCount);
    //return rowCount;
  }

	@Override
  public RelOptCost getCumulativeCost(RelNode rel) {
    if (COST_MODEL_NAME.equals("rowCount")) {
      // FIXME: is this actually just rowCount w/o any modifications?
      RelOptCost origCost = super.getCumulativeCost(rel);
      return origCost;
    }

    if (rel instanceof Join) {
      RelNode left = ((Join) rel).getLeft();
      RelNode right = ((Join) rel).getRight();
      RelOptCost leftCost = getCumulativeCost(left);
      RelOptCost rightCost = getCumulativeCost(right);
      RelOptCost curCost = getNonCumulativeCost(rel);

      if (COST_MODEL_NAME.equals("CM2")) {
        return leftCost.plus(rightCost).plus(curCost);
      } else {
        // FIXME:
        System.err.println("unsupported cost model " + COST_MODEL_NAME);
        System.exit(-1);
      }
    }
    // else just treat it as usual
    RelOptCost origCost = super.getCumulativeCost(rel);
    return origCost;
  }

	@Override
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    RelOptCost orig_cost = super.getNonCumulativeCost(rel);
    if (rel instanceof Join && !COST_MODEL_NAME.equals("rowCount")) {
      RelNode left = ((Join) rel).getLeft();
      RelNode right = ((Join) rel).getRight();
      double leftRows = getRowCount(left);
      double rightRows = getRowCount(right);
      double curRows = getRowCount(rel);
      if (COST_MODEL_NAME.equals("CM2")) {
        // Now, the cost model will consider whether the leftRows and rightRows
        // fit in the memory or not.
        if ((leftRows + rightRows) < MEMORY_LIMIT) {
          return orig_cost;
        } else if (Math.min(leftRows, rightRows) < MEMORY_LIMIT) {
          //System.out.println("!!!case 2!!!");
          double newCost = 2*(leftRows + rightRows) + curRows;
          ((MyCost) orig_cost).cost = newCost;
        } else {
          //System.out.println("!!!case 3!!!");
          double newCost = rightRows + Math.ceil(rightRows / MEMORY_LIMIT) * leftRows + curRows;
          ((MyCost) orig_cost).cost = newCost;
        }
      }
    }
    return orig_cost;
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
			System.out.println(e);
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
      System.out.println(e);
    }
    return null;
  }
}

