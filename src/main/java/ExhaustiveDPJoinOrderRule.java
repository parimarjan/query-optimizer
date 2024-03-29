import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.util.*;

import static org.apache.calcite.util.mapping.Mappings.TargetMapping;

// new ones
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.rel.core.*;

// experimental
import org.apache.calcite.plan.RelOptUtil;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.*;

/**
 */
public class ExhaustiveDPJoinOrderRule extends RelOptRule
{
  public static final ExhaustiveDPJoinOrderRule INSTANCE =
      new ExhaustiveDPJoinOrderRule(RelFactories.LOGICAL_BUILDER);

  String queryName = null;
  public void setQueryName(String queryName) {
    //System.out.println("setQueryName");
    this.queryName = queryName;
  }

  private class IntermediateJoinState {
    ArrayList<ImmutableBitSet[]> bestJoins;
    double cost;
    public IntermediateJoinState(ArrayList<ImmutableBitSet[]> bestJoins, double
        cost)
    {
      this.bestJoins = bestJoins;
      this.cost = cost;
    }
  }

  // The keys represent a set of factors in the original vertices of the
  // QueryGraph. The values represent a sequence of edges (it's index at each
  // stage in QueryGraph.edges) that are chosen for the optimal memoized
  // ordering for the given set of factors. We can use these to reconstruct the
  // QueryGraph with a sequence of updateGraph steps for each
  // of the edge
  //private HashMap<ImmutableBitSet, ArrayList<ImmutableBitSet[]>> memoizedBestJoins;
  private HashMap<ImmutableBitSet, IntermediateJoinState> memoizedBestJoins;

  /** Creates an ExhaustiveDPJoinOrderRule. */
  public ExhaustiveDPJoinOrderRule(RelBuilderFactory relBuilderFactory) {
    super(operand(MultiJoin.class, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public ExhaustiveDPJoinOrderRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(joinFactory, projectFactory));
  }

	/*
	 * We follow the algorithm described at:
   * https://dsg.uwaterloo.ca/seminars/notes/Guido.pdf
	 */
  @Override
  public void onMatch(RelOptRuleCall call)
  {
    //System.out.println("onMatch");
    RelNode orig = call.getRelList().get(0);
    call.getPlanner().setImportance(orig, 0.0);
    //memoizedBestJoins = new HashMap<ImmutableBitSet, ArrayList<ImmutableBitSet[]>>();
    memoizedBestJoins = new HashMap<ImmutableBitSet, IntermediateJoinState>();
    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final MyMetadataQuery mq = MyMetadataQuery.instance();
    if (queryName != null) {
      mq.setQueryName(queryName);
      System.out.println("mq.SetQueryName done");
    }

    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
			 // no edges have been chosen yet, so we add an empty list
			 memoizedBestJoins.put(ImmutableBitSet.range(i, i+1),
           new IntermediateJoinState(new ArrayList<ImmutableBitSet[]>(), 0.00));
    }
    //System.out.println("first loop done");

    //QueryGraph curQG = new QueryGraph(multiJoin, mq, rexBuilder, call.builder());
    QueryGraph curQG;
    QueryGraph startQG = new QueryGraph(multiJoin, mq, rexBuilder, call.builder());
    //System.out.println("startQG created");
    // iterates over the csgCmp pairs as defined in thomas neumann's notes
    Iterator<ImmutableBitSet[]> csgCmpIt = startQG.csgCmpIterator();
    //System.out.println("startQG.csgCmpIterator started");
    while (csgCmpIt.hasNext())
    {
      ImmutableBitSet[] curPair = csgCmpIt.next();
      ImmutableBitSet S1 = curPair[0];
      ImmutableBitSet S2 = curPair[1];
      ImmutableBitSet S = S1.union(S2);
      ArrayList<ImmutableBitSet[]> p1 = memoizedBestJoins.get(S1).bestJoins;
      ArrayList<ImmutableBitSet[]> p2 = memoizedBestJoins.get(S2).bestJoins;
      assert p1 != null;
      assert p2 != null;

      curQG = new QueryGraph(multiJoin, mq, rexBuilder, call.builder());
      //System.out.println("new curQG created");
      //curQG.reset(multiJoin, mq, rexBuilder, call.builder());

      // NOTE: S1, and S2, are two subgraphs with no common elements. So we can
      // execute the optimal ordering for those (p1, and p2) in either order,
      // since the factors are completely independent, thus joining factors in
      // one subgraph will NOT affect the indices of the factors in the other
      // subgraph.
      for (ImmutableBitSet[] factors : p1) {
        curQG.updateGraphBitset(factors);
      }

      int factor1Idx = curQG.allVertexes.size()-1;
      for (ImmutableBitSet[] factors : p2) {
        curQG.updateGraphBitset(factors);
      }

      // last vertex added must be the one because of factors in p2.
      int factor2Idx = curQG.allVertexes.size()-1;
      assert factor1Idx != factor2Idx;

      // now, cost of joining the two latest nodes.
      ImmutableBitSet[] lastFactors = {S1, S2};
      ImmutableBitSet[] lastFactors2 = {S2, S1};
      double cost1 = curQG.calculateCostBitset(lastFactors);
      double cost2 = curQG.calculateCostBitset(lastFactors2);
      if (cost1 < cost2) curQG.updateGraphBitset(lastFactors);
      else curQG.updateGraphBitset(lastFactors2);

      double curCost = curQG.costSoFar;

      IntermediateJoinState bestOrder = memoizedBestJoins.get(S);
      ArrayList<ImmutableBitSet[]> curOrder = new ArrayList<ImmutableBitSet[]>();
      curOrder.addAll(p1);
      curOrder.addAll(p2);
      curOrder.add(lastFactors);
      if (bestOrder == null) {
        // first time we see the subgraph S
        memoizedBestJoins.put(S, new IntermediateJoinState(curOrder, curCost));
      } else {
        // find the cost of bestOrder, and replace if it needed.
        if (bestOrder.cost > curCost) {
          memoizedBestJoins.put(S, new IntermediateJoinState(curOrder, curCost));
        }
      }
    }

    // Not checking for null here, as we MUST have this in memoizedBestJoins,
    // or else something is wrong with the algorithm and it might as well
    // crash.
    ArrayList<ImmutableBitSet[]> optOrdering =
      memoizedBestJoins.get(ImmutableBitSet.range(0,
            multiJoin.getNumJoinFactors())).bestJoins;
    QueryGraph finalQG = new QueryGraph(multiJoin, mq, rexBuilder, call.builder());

    Query curQuery = QueryOptExperiment.getCurrentQuery();

    if (curQuery != null) {
      //System.out.println("curQuery NOT NULL");
      // in testCardinalities case, we do not need to set these up
      curQuery.joinOrders.put("EXHAUSTIVE", new MyUtils.JoinOrder());
      HashMap<ArrayList<String>, Double> optCosts =
                new HashMap<ArrayList<String>, Double>();
      ArrayList<int[]> joinOrder = new ArrayList<int[]>();

      for (ImmutableBitSet[] factors : optOrdering) {
        int[] factorIndices = finalQG.updateGraphBitset(factors);
        joinOrder.add(factorIndices);
        optCosts.put(finalQG.getLastNodeTables(), finalQG.lastCost);
      }
      curQuery.joinOrders.get("EXHAUSTIVE").joinEdgeChoices = joinOrder;
      curQuery.joinOrders.get("EXHAUSTIVE").joinCosts = optCosts;
    } else {
      for (ImmutableBitSet[] factors : optOrdering) {
        int[] factorIndices = finalQG.updateGraphBitset(factors);
      }
    }

    // before calling this, we need to have completed the updateGraphBitset
    // loop
    RelNode optNode = finalQG.getFinalOptimalRelNode();
    call.transformTo(optNode);
  }
}

// End ExhaustiveDPJoinOrderRule.java

