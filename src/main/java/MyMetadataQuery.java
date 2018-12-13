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

public class MyMetadataQuery extends RelMetadataQuery {

  // in terms of number of rows. This is used for calculating the cost in a
  // non-linear model.
  private final double MEMORY_LIMIT = Math.pow(10, 2);
  // TODO: support options: CM1, CM2, CM3, RowCount
  private final String COST_MODEL_NAME;

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
  public RelOptCost getCumulativeCost(RelNode rel) {
    //System.out.println("memory limit is: " + MEMORY_LIMIT);
    if (rel instanceof Join && !COST_MODEL_NAME.equals("")) {
      RelNode left = ((Join) rel).getLeft();
      RelNode right = ((Join) rel).getRight();
      RelOptCost leftCost = getCumulativeCost(left);
      RelOptCost rightCost = getCumulativeCost(right);
      RelOptCost curCost = getNonCumulativeCost(rel);

      if (COST_MODEL_NAME.equals("CM2")) {
        return leftCost.plus(rightCost).plus(curCost);
      } else {
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
        } else if (Math.min(leftRows, rightRows) < (Math.pow(MEMORY_LIMIT, 1))) {
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
}

