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

public class MyMetadataQuery extends RelMetadataQuery {

  // in terms of number of rows. This is used for calculating the cost in a
  // non-linear model.
  private final int MEMORY_LIMIT = 1000;
  private final int OVERFLOW_PENALTY = MEMORY_LIMIT*2;
  private final int NON_LINEAR_METHOD = 3;

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
  }

	@Override
	public Double getRowCount(RelNode rel) {
    Double orig_count = super.getRowCount(rel);
    // now let us do some sort of non-linear modeling here?
    return orig_count;
  }

	public Double getNonLinearCount(double count) {
    // overflow chunks
    double cost = 0;
    if (NON_LINEAR_METHOD == 1) {
      cost = count * 2.0;
    } else if (NON_LINEAR_METHOD == 2) {
      int numOverflows = ((int) count) / MEMORY_LIMIT;
      cost = count + numOverflows * OVERFLOW_PENALTY;
    } else if (NON_LINEAR_METHOD == 3) {
      int numOverflows = ((int) count) / MEMORY_LIMIT;
      // FIXME: add random offset?
      cost = (count/1e8) + numOverflows * OVERFLOW_PENALTY;
    }
    return cost;
  }

  public RelOptCost getCumulativeCost2(RelNode rel) {
    // FIXME: cast this safely
    TestCost orig_cost = (TestCost) super.getCumulativeCost(rel);
    double rows = orig_cost.getRows();
    return new TestCost(getNonLinearCount(rows), 0.0, 0.0);
  }

	@Override
  public RelOptCost getCumulativeCost(RelNode rel) {
    // FIXME: cast this safely
    RelOptCost orig_cost = super.getCumulativeCost(rel);
    return orig_cost;
  }

  public RelOptCost getCumulativeCost(RelNode rel, boolean isNonLinearCostModel) {
    if (isNonLinearCostModel) {
      return getCumulativeCost2(rel);
    } else {
      return getCumulativeCost(rel);
    }
  }

  public RelOptCost getNonCumulativeCost(RelNode rel, boolean isNonLinearCostModel) {
    RelOptCost orig_cost = super.getNonCumulativeCost(rel);
    double rows = orig_cost.getRows();

    if (isNonLinearCostModel) {
      return new TestCost(getNonLinearCount(rows), 0.0, 0.0);
    } else {
      return orig_cost;
    }
  }
}

