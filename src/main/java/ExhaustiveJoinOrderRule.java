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
    //System.out.println("exhaustive join rule!");
    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    multiJoin = new LoptMultiJoin2(multiJoinRel);
    mq = MyMetadataQuery.instance();

    final List<QueryGraphUtils.Vertex> vertexes = new ArrayList<>();

    // Add the orignal tables as vertexes
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      // this is a vertex, so must be one of the tables from the database
      double cost = mq.getRowCount(rel);
      QueryGraphUtils.Vertex newVertex = new QueryGraphUtils.LeafVertex(i, rel, cost, x);
      vertexes.add(newVertex);
      x += rel.getRowType().getFieldCount();
    }
    assert x == multiJoin.getNumTotalFields();

    final List<LoptMultiJoin2.Edge> unusedEdges = new ArrayList<>();
    for (RexNode node : multiJoin.getJoinFilters()) {
      unusedEdges.add(multiJoin.createEdge2(node));
    }
    final List<LoptMultiJoin2.Edge> usedEdges = new ArrayList<>();
    numAdded = 0;
    recursiveAddNodes(call, vertexes, usedEdges, unusedEdges, rexBuilder);
  }

  private void recursiveAddNodes(RelOptRuleCall call, List<QueryGraphUtils.Vertex> vertexes, List<LoptMultiJoin2.Edge> usedEdges, List<LoptMultiJoin2.Edge> unusedEdges, RexBuilder rexBuilder)
  {
    //System.out.println("recursive call, size = " + unusedEdges.size());
    /* Break condition */
    if (unusedEdges.size() == 0) {
      RelBuilder relBuilder = call.builder();
      // FIXME: check that we don't need to change this.
      //rexBuilder = multiJoinRel.getCluster().getRexBuilder();

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
      //System.out.println("adding another equivalent node");
      numAdded += 1;
      if (numAdded % 100 == 0) {
        System.out.println("numAdded exhaustive search nodes: " + numAdded);
      }
      call.transformTo(optNode);
      return;
    }

    for (int i = 0; i < unusedEdges.size(); i++) {
      // create local copies of the input arrays at the current level of recursion
      List<QueryGraphUtils.Vertex> curVertexes = new ArrayList(vertexes);
      List<LoptMultiJoin2.Edge> curUsedEdges = new ArrayList(usedEdges);
      List<LoptMultiJoin2.Edge> curUnusedEdges = new ArrayList(unusedEdges);

      final LoptMultiJoin2.Edge curEdge = curUnusedEdges.get(i);
      assert curEdge.factors.cardinality() == 2;
      int [] factors = curEdge.factors.toArray();
      // this will take care of correctly updating the arrays to reflect our
      // choice
      qGraphUtils.updateGraph(curVertexes, factors, curUsedEdges, curUnusedEdges, mq,
          rexBuilder);
      recursiveAddNodes(call, curVertexes, curUsedEdges, curUnusedEdges, rexBuilder);
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

