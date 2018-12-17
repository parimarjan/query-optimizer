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
 */
public class ExhaustiveJoinOrderRule extends RelOptRule {
  public static final ExhaustiveJoinOrderRule INSTANCE =
      new ExhaustiveJoinOrderRule(RelFactories.LOGICAL_BUILDER);

  // TODO: just set this to what we want.
  //private final PrintWriter pw = Util.printWriter(System.out);
  private final PrintWriter pw = null;
  private QueryGraphUtils qGraphUtils = new QueryGraphUtils(pw);
  private LoptMultiJoin2 multiJoin;
  private MyMetadataQuery mq;
  private int numAdded = 0;
  // just some large positive number to initialize, costs always positive, and
  // lower is better.
  private double bestCost = 1000000000.00;
  //private List<QueryGraphUtils.Vertex> bestVertexes = null;
  private RelNode bestOptNode = null;
  private int totalOpts;

  /** Creates an ExhaustiveJoinOrderRule. */
  public ExhaustiveJoinOrderRule(RelBuilderFactory relBuilderFactory) {
    super(operand(MultiJoin.class, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public ExhaustiveJoinOrderRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(joinFactory, projectFactory));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    bestCost = 1000000000.00;
    bestOptNode = null;
    totalOpts = 0;
    // Setting original expressions importance to 0, so our choice will be
    // chosen.
    RelNode orig = call.getRelList().get(0);
    call.getPlanner().setImportance(orig, 0.0);

    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    multiJoin = new LoptMultiJoin2(multiJoinRel);
    mq = MyMetadataQuery.instance();

    final List<QueryGraphUtils.Vertex> vertexes = new ArrayList<>();

    // Add the orignal tables as vertexes
    int x = 0;
    double totalVCost = 0.00;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      // this is a vertex, so must be one of the tables from the database
      double cost = mq.getRowCount(rel);
      totalVCost += cost;
      QueryGraphUtils.Vertex newVertex = new QueryGraphUtils.LeafVertex(i, rel, cost, x);
      vertexes.add(newVertex);
      x += rel.getRowType().getFieldCount();
    }
    System.out.println("cost of orig vertices: " + totalVCost);
    assert x == multiJoin.getNumTotalFields();

    final List<LoptMultiJoin2.Edge> unusedEdges = new ArrayList<>();
    for (RexNode node : multiJoin.getJoinFilters()) {
      LoptMultiJoin2.Edge edge = multiJoin.createEdge2(node);
      boolean newEdge = true;
      //System.out.println("edge.factors: " + edge.factors);
      for (LoptMultiJoin2.Edge edge2 : unusedEdges) {
        //System.out.println("edge2.factors: " + edge2.factors);
        if (edge2.factors.contains(edge.factors)) {
          System.out.println("!!going to merge an edge!!");
          newEdge = false;
          // combine these edges
          edge2.mergeEdge(edge, rexBuilder);
          break;
        }
      }
      if (newEdge) unusedEdges.add(edge);
    }
    System.out.println("final length of unusedEdges: " + unusedEdges.size());
    // FIXME: temporary
    if (unusedEdges.size() >= 13) {
      // don't want to do this!
      return;
    }

    final List<LoptMultiJoin2.Edge> usedEdges = new ArrayList<>();
    numAdded = 0;
    recursiveAddNodes(call, vertexes, usedEdges, unusedEdges, rexBuilder, 0.00);
    assert bestOptNode != null;
    //assert bestVertexes != null;
    // build a final optimized node using the bestVertexes
    System.out.println("bestCost: " + bestCost);
    System.out.println("exhaustive search optNode cost " + mq.getCumulativeCost(bestOptNode));
    call.transformTo(bestOptNode);
  }

  private void recursiveAddNodes(RelOptRuleCall call, List<QueryGraphUtils.Vertex> vertexes, List<LoptMultiJoin2.Edge> usedEdges, List<LoptMultiJoin2.Edge> unusedEdges, RexBuilder rexBuilder, double costSoFar)
  {
    totalOpts += 1;
    if (totalOpts % 10000 == 0) {
      System.out.println("totalOpts = " + totalOpts);
    }
    //System.out.println("size of unusedEdges: " + unusedEdges.size());
    //System.out.println("costSoFar: " + costSoFar);
    //System.out.println("recursive call, size = " + unusedEdges.size());
    /* Break condition */
    if (unusedEdges.size() == 0) {
      // have reached the end of recursion, and costSoFar is also better than
      // bestCost seen so far. So this must be the best node so far.
      //bestCost = costSoFar;
      RelBuilder relBuilder = call.builder();

      // celebrate and add the relNode being developed so far from optRelNodes
      // to the set of equivalent nodes.
      List<Pair<RelNode, TargetMapping>> optRelNodes = new ArrayList<>();
      for (QueryGraphUtils.Vertex v : vertexes) {
        qGraphUtils.updateRelNodes(v, optRelNodes, rexBuilder, relBuilder, multiJoin);
      }
      final Pair<RelNode, Mappings.TargetMapping> top = Util.last(optRelNodes);
      relBuilder.push(top.left)
          .project(relBuilder.fields(top.right));
      RelNode optNode = relBuilder.build();
      double trueCost = ((MyCost) mq.getCumulativeCost(optNode)).getCost();
      if (trueCost < bestCost) {
        // update it ONLY now.
        numAdded += 1;
        bestCost = trueCost;
        bestOptNode = optNode;
        if (numAdded % 5 == 0) {
          System.out.println("numAdded exhaustive search nodes: " + numAdded);
        }
      }
      return;
    }

    for (int i = 0; i < unusedEdges.size(); i++) {
      // create local copies of the input arrays at the current level of recursion
      List<QueryGraphUtils.Vertex> curVertexes = vertexes;
      List<LoptMultiJoin2.Edge> curUsedEdges = new ArrayList(usedEdges);
      List<LoptMultiJoin2.Edge> curUnusedEdges = new ArrayList(unusedEdges);
      double curCost = costSoFar;

      final LoptMultiJoin2.Edge curEdge = curUnusedEdges.get(i);
      // FIXME: should not be two right?
      assert curEdge.factors.cardinality() == 2;
      int [] factors = curEdge.factors.toArray();
      // this will take care of correctly updating the arrays to reflect our
      // choice
      double estCost = qGraphUtils.updateGraph(curVertexes, factors,
          curUsedEdges, curUnusedEdges, mq, rexBuilder);
      // FIXME: decompose vertexes -> relNode function
      List<Pair<RelNode, TargetMapping>> curRelNodes = new ArrayList<>();
      RelBuilder rBuilder = call.builder();
      for (QueryGraphUtils.Vertex v : curVertexes) {
        qGraphUtils.updateRelNodes(v, curRelNodes, rexBuilder, rBuilder, multiJoin);
      }
      Pair<RelNode, Mappings.TargetMapping> curTop = Util.last(curRelNodes);
      // FIXME: is it ok to not have any of these projections?
      rBuilder.push(curTop.left);
          //.project(rBuilder.fields(curTop.right));
      RelNode curOptNode = rBuilder.build();

      curCost += ((MyCost) mq.getNonCumulativeCost(curOptNode)).getCost();

      if (curCost >= bestCost) {
        //System.out.println("pruning branch!, numAdded = " + numAdded + " i = " + i);
        continue;
      }
      recursiveAddNodes(call, curVertexes, curUsedEdges, curUnusedEdges, rexBuilder, curCost);
      // TODO: I believe the behaviour will be correct even without removing
      // the last element as the updated edges will be pointing to the right
      // factors in each search branch. But just in case. Can look further into
      // this to ensure I understand it correctly.
      curVertexes.remove(curVertexes.size() - 1);
    }
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
}

// End ExhaustiveJoinOrderRule.java

