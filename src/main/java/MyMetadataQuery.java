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
  private final int MEMORY_LIMIT = 10^4;
  //private final int OVERFLOW_PENALTY = MEMORY_LIMIT*2;
  //private final int NON_LINEAR_METHOD = 3;
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
    RelOptCost orig_cost = super.getCumulativeCost(rel);
    return orig_cost;
  }

	@Override
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    RelOptCost orig_cost = super.getNonCumulativeCost(rel);
    return orig_cost;
  }

}

