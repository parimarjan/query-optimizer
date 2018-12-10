/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
 * TODO: describe bushy rule which served as the base template etc.
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 * <p>It is triggered by the pattern
 * {@link org.apache.calcite.rel.logical.LogicalProject} ({@link MultiJoin}).
 *
 * <p>It is similar to
 * {@link org.apache.calcite.rel.rules.LoptOptimizeJoinRule}.
 * {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 *
 * <p>TODO:
 * <ol>
 *   <li>Join conditions that touch 1 factor.
 *   <li>Join conditions that touch 3 factors.
 *   <li>More than 1 join conditions that touch the same pair of factors,
 *       e.g. {@code t0.c1 = t1.c1 and t1.c2 = t0.c3}
 * </ol>
 */
public class RLJoinOrderRule extends RelOptRule {
  public static final RLJoinOrderRule INSTANCE =
      new RLJoinOrderRule(RelFactories.LOGICAL_BUILDER);

  // TODO: just set this to what we want.
  //private final PrintWriter pw = Util.printWriter(System.out);
  private final PrintWriter pw = null;
  private boolean isNonLinearCostModel;
  private boolean onlyFinalReward;

  /** Creates an RLJoinOrderRule. */
  public RLJoinOrderRule(RelBuilderFactory relBuilderFactory) {
    super(operand(MultiJoin.class, any()), relBuilderFactory, null);
  }

  private void printStuff(RelNode rel) {
    if (rel == null) {
      System.out.println("rel was null");
      return;
    }
    System.out.println("rel class: " + rel.getClass().getName());
    System.out.println("digest: " + rel.recomputeDigest());
    if (rel instanceof RelSubset) {
      RelSubset s = (RelSubset) rel;
      printStuff(s.getOriginal());
    } else if (rel instanceof Filter) {
      System.out.println("filter!");
      printStuff(rel.getInput(0));
    } else if (rel instanceof TableScan) {
      System.out.println("table scan!");
      System.out.println("table name: " + rel.getTable().getQualifiedName());
    }
  }

  @Deprecated // to be removed before 2.0
  public RLJoinOrderRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(joinFactory, projectFactory));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    // setting original expression's importance to 0
    RelNode orig = call.getRelList().get(0);
    call.getPlanner().setImportance(orig, 0.0);

    ZeroMQServer zmq = QueryOptExperiment.getZMQServer();
    // this is required if we want to use node.computeSelfCost()
    // final RelOptPlanner planner = call.getPlanner();
    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    isNonLinearCostModel = QueryOptExperiment.isNonLinearCostModel();
    onlyFinalReward = QueryOptExperiment.onlyFinalReward();
		// wrapper around RelMetadataQuery, to add support for non linear cost
    // models.
    final MyMetadataQuery mq = MyMetadataQuery.instance();

    final LoptMultiJoin2 multiJoin = new LoptMultiJoin2(multiJoinRel);
    final List<QueryGraphUtils.Vertex> vertexes = new ArrayList<>();
    int x = 0;
    Double scanCost = 0.0;
    // Sequence of relNodes that are used to build the final optimized relNode
    // one join at a time.
    List<Pair<RelNode, TargetMapping>> optRelNodes = new ArrayList<>();
    QueryGraphUtils qGraphUtils = new QueryGraphUtils(pw);

    // Add the orignal tables as vertexes
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      // this is a vertex, so must be one of the tables from the database
      double cost = mq.getRowCount(rel);
      if (isNonLinearCostModel) {
        cost = mq.getNonLinearCount(cost);
      }
      scanCost += cost;
      QueryGraphUtils.Vertex newVertex = new QueryGraphUtils.LeafVertex(i, rel, cost, x);
      vertexes.add(newVertex);
      qGraphUtils.updateRelNodes(newVertex, optRelNodes, rexBuilder, relBuilder, multiJoin);
      x += rel.getRowType().getFieldCount();
    }
    zmq.scanCost = scanCost;
    assert x == multiJoin.getNumTotalFields();

    final List<LoptMultiJoin2.Edge> unusedEdges = new ArrayList<>();
    for (RexNode node : multiJoin.getJoinFilters()) {
      unusedEdges.add(multiJoin.createEdge2(node));
    }
    // In general, we can keep updating it after every edge collapse, although
    // it shouldn't change for the way DQ featurized.
    zmq.state = new ArrayList<Integer>(mapToQueryFeatures(DbInfo.getCurrentQueryVisibleFeatures(), multiJoin).toList());
    final List<LoptMultiJoin2.Edge> usedEdges = new ArrayList<>();
    // only used for finalReward scenario
    Double costSoFar = 0.00;
    Double estCostSoFar = 0.00;
    for (;;) {
      // break condition
      final int[] factors;
      if (unusedEdges.size() == 0) {
        // No more edges. Are there any un-joined vertexes?
        zmq.episodeDone = 1;
        final QueryGraphUtils.Vertex lastVertex = Util.last(vertexes);
        final int z = lastVertex.factors.previousClearBit(lastVertex.id - 1);
        if (z < 0) {
          break;
        }
        factors = new int[] {z, lastVertex.id};
      } else {
        // TODO: make this an externally provided function to choose the next
        // edge.
        zmq.episodeDone = 0;
        factors = chooseNextEdge(unusedEdges, vertexes, multiJoin);
      }

      double estCost = qGraphUtils.updateGraph(vertexes, factors, usedEdges,
          unusedEdges, mq, rexBuilder);
      estCostSoFar += estCost;
      qGraphUtils.updateRelNodes(Util.last(vertexes), optRelNodes, rexBuilder, relBuilder, multiJoin);
      // FIXME: do we need a new relBuilder here?
      //RelBuilder tmpRelBuilder = call.builder();
      //RelBuilder tmpRelBuilder = relBuilder;
      Pair<RelNode, Mappings.TargetMapping> curTop = Util.last(optRelNodes);
      //tmpRelBuilder.push(curTop.left)
          //.project(tmpRelBuilder.fields(curTop.right));
      //RelNode curOptNode = tmpRelBuilder.build();
      RelNode curOptNode = curTop.left;
      double cost = mq.getNonCumulativeCost(curOptNode).getRows();
      //double cost = estCost;

      if (!onlyFinalReward) {
        costSoFar += cost;
        zmq.lastReward = -cost;
      } else {
        // reward will be 0.00 until the very end when we throw everything at
        // them.
        zmq.lastTrueReward = -cost;
        costSoFar += cost;
        if (unusedEdges.size() == 0) {
          zmq.lastReward = -costSoFar;
        } else {
          zmq.lastReward = 0.00;
        }
      }
      zmq.waitForClientTill("getReward");
    }

    //System.out.println("costSoFar: " + costSoFar);
    //System.out.println("estCostSoFar: " + estCostSoFar);

    /// FIXME: need to understand what this TargetMapping business really is...
    /// just adding a projection on top of the left nodes we had.
    final Pair<RelNode, Mappings.TargetMapping> top = Util.last(optRelNodes);
    relBuilder.push(top.left)
        .project(relBuilder.fields(top.right));
    RelNode optNode = relBuilder.build();
    //System.out.println("RL optNode cost: " + mq.getCumulativeCost(optNode));
    call.transformTo(optNode);
  }

  private void trace(List<QueryGraphUtils.Vertex> vertexes,
      List<LoptMultiJoin2.Edge> unusedEdges, List<LoptMultiJoin2.Edge> usedEdges,
      int edgeOrdinal, PrintWriter pw)
  {
    pw.println("bestEdge: " + edgeOrdinal);
    pw.println("vertexes:");
    for (QueryGraphUtils.Vertex vertex : vertexes) {
      pw.println(vertex);
    }
    pw.println("unused edges:");
    for (LoptMultiJoin2.Edge edge : unusedEdges) {
      pw.println(edge);
    }
    pw.println("edges:");
    for (LoptMultiJoin2.Edge edge : usedEdges) {
      pw.println(edge);
    }
    pw.println();
    pw.flush();
  }


  private Pair<ArrayList<Integer>, ArrayList<Integer>> getDQFeatures(LoptMultiJoin2.Edge edge, List<QueryGraphUtils.Vertex> vertexes, LoptMultiJoin2 mj)
  {
    boolean onlyJoinConditionAttributes = false;
    ArrayList<Integer> left = null;
    ArrayList<Integer> right = null;

    Pair<ArrayList<Integer>, ArrayList<Integer>> pair;
    List<Integer> factors = edge.factors.toList();

    // intersect this with the features present in the complete query.
    ImmutableBitSet queryFeatures = DbInfo.getCurrentQueryVisibleFeatures();

    for (Integer factor : factors) {
      QueryGraphUtils.Vertex v = vertexes.get(factor);
      ImmutableBitSet.Builder fBuilder = ImmutableBitSet.builder();
      ImmutableBitSet.Builder allPossibleFeaturesBuilder = ImmutableBitSet.builder();
      // all the join conditions for this edge.
      allPossibleFeaturesBuilder.addAll(edge.columns);
      assert v != null;
      assert v.visibleFeatures != null;
      ImmutableBitSet allPossibleFeatures = allPossibleFeaturesBuilder.build();
      ImmutableBitSet conditionFeatures = allPossibleFeatures.intersect(v.visibleFeatures);
      // now we have only the features visible in join condition.
      //System.out.println("should only have one visible: " + conditionFeatures);
      fBuilder.addAll(conditionFeatures);
      fBuilder.addAll(v.visibleFeatures);
      ImmutableBitSet features = fBuilder.build();
      //System.out.println("all possible features: " + features);
      features = features.intersect(queryFeatures);
      //System.out.println("exact features: " + features);
      features = mapToQueryFeatures(features, mj);
      //System.out.println("mapped features: " + features);
      // both start of as null. Assume first guy is the left one.
      if (left == null) {
        left = new ArrayList<Integer>(features.toList());
      } else {
        right = new ArrayList<Integer>(features.toList());
      }
    }
    return new Pair<ArrayList<Integer>, ArrayList<Integer>>(left, right);
  }

  private ImmutableBitSet mapToQueryFeatures(ImmutableBitSet bs, LoptMultiJoin2 mj)
  {
    ImmutableBitSet.Builder featuresBuilder = ImmutableBitSet.builder();
    for (Integer i : bs) {
      featuresBuilder.set(mj.mapToDatabase.get(i));
    }
    return featuresBuilder.build();
  }

  /*
   * Passes control to the python agent to choose the next edge.
   * @ret: factors associated with the chosen edge
   */
  private int [] chooseNextEdge(List<LoptMultiJoin2.Edge> unusedEdges,
      List<QueryGraphUtils.Vertex> vertexes, LoptMultiJoin2 multiJoin)
  {
    final int[] factors;
    ZeroMQServer zmq = QueryOptExperiment.getZMQServer();
    // each edge is equivalent to a possible action, and must be represented
    // by its features
    final int edgeOrdinal;
    ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>> actionFeatures = new ArrayList<Pair<ArrayList<Integer>, ArrayList<Integer>>>();
    for (LoptMultiJoin2.Edge edge : unusedEdges) {
      Pair<ArrayList<Integer>, ArrayList<Integer>> features = getDQFeatures(edge, vertexes, multiJoin);
      actionFeatures.add(features);
    }
    zmq.actions = actionFeatures;
    zmq.waitForClientTill("step");
    if (zmq.reset) {
      // Technically, we should allow this situation -- reset being called
      // in the middle of an episode, but for now, we complain here because
      // this should not be happening in training.
      edgeOrdinal = ThreadLocalRandom.current().nextInt(0, unusedEdges.size());
      System.out.println("actions should be chosen by python agent and not randomly!");
    } else {
      edgeOrdinal = zmq.nextAction;
    }
    //System.out.println("edgeOrdinal chosen: " + edgeOrdinal);
    final LoptMultiJoin2.Edge bestEdge = unusedEdges.get(edgeOrdinal);

    // For now, assume that the edge is between precisely two factors.
    // 1-factor conditions have probably been pushed down,
    // and 3-or-more-factor conditions are advanced. (TODO:)
    // Therefore, for now, the factors that are merged are exactly the
    // factors on this edge.
    assert bestEdge.factors.cardinality() == 2;
    factors = bestEdge.factors.toArray();
    return factors;
  }

  private void collapseEdges()
  {
    // FIXME: finish attempt to collapse common edges.
    //final List<RexNode> conditions = new ArrayList<>();
    //final Iterator<LoptMultiJoin2.Edge> edgeIterator1 = unusedEdges.iterator();
    //final Iterator<LoptMultiJoin2.Edge> edgeIterator2 = unusedEdges.iterator();
    //final List<LoptMultiJoin2.Edge> unusedEdges2 = new ArrayList<>();
    //while (edgeIterator1.hasNext()) {
      //LoptMultiJoin2.Edge edge = edgeIterator1.next();
      //ImmutableBitSet edgeFactors = edge.factors;
      //while (edgeIterator2.hasNext()) {
        //LoptMultiJoin2.Edge edge2 = edgeIterator2.next();
        //if (edge.toString().equals(edge2.toString())) continue;
        //if (!edgeFactors.contains(edge2.factors)) continue;
        //// time to collapse the edges!
      //}
    //}
    //System.out.println("size of unusedEdges: " + unusedEdges.size());
  }
}

// End RLJoinOrderRule.java

