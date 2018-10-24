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

// experimental
import org.apache.calcite.plan.RelOptUtil;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.plan.*;

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
  private final PrintWriter pw = Util.printWriter(System.out);

  /** Creates an RLJoinOrderRule. */
  public RLJoinOrderRule(RelBuilderFactory relBuilderFactory) {
    super(operand(MultiJoin.class, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public RLJoinOrderRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(joinFactory, projectFactory));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final RelMetadataQuery mq = call.getMetadataQuery();

    final LoptMultiJoin2 multiJoin = new LoptMultiJoin2(multiJoinRel);

    final List<Vertex> vertexes = new ArrayList<>();
/// PN: LeafVertex is just the data sources?
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      // this is a vertex, so must be one of the tables from the database
      RelOptTable tbl = rel.getTable();
      if (tbl != null) {
        List<String> tblNames = tbl.getQualifiedName();

        for (String s : tblNames) {
          System.out.println(i + "th' table name is: " + s);
        }
      } else System.out.println("table was null");

      double cost = mq.getRowCount(rel);
      vertexes.add(new LeafVertex(i, rel, cost, x));
      x += rel.getRowType().getFieldCount();
    }
    System.exit(-1);

    assert x == multiJoin.getNumTotalFields();

    final List<LoptMultiJoin2.Edge> unusedEdges = new ArrayList<>();
    for (RexNode node : multiJoin.getJoinFilters()) {
      unusedEdges.add(multiJoin.createEdge(node));
    }

    /// TODO: test other costs here.
    // Comparator that chooses the best edge. A "good edge" is one that has
    // a large difference in the number of rows on LHS and RHS.
    final Comparator<LoptMultiJoin2.Edge> edgeComparator =
        new Comparator<LoptMultiJoin2.Edge>() {
          public int compare(LoptMultiJoin2.Edge e0, LoptMultiJoin2.Edge e1) {
            return Double.compare(rowCountDiff(e0), rowCountDiff(e1));
          }

          private double rowCountDiff(LoptMultiJoin2.Edge edge) {
            assert edge.factors.cardinality() == 2 : edge.factors;
            final int factor0 = edge.factors.nextSetBit(0);
            final int factor1 = edge.factors.nextSetBit(factor0 + 1);
            return Math.abs(vertexes.get(factor0).cost
                - vertexes.get(factor1).cost);
          }
        };

    final List<LoptMultiJoin2.Edge> usedEdges = new ArrayList<>();
    for (;;) {
      //final int edgeOrdinal = chooseBestEdge(unusedEdges, edgeComparator);
      /// Test out a random strategy.
      final int edgeOrdinal;
      if (unusedEdges.size() >= 1) {
        edgeOrdinal = ThreadLocalRandom.current().nextInt(0, unusedEdges.size());
      } else edgeOrdinal = -1;

// PN: printouts might be very helpful.
      if (pw != null) {
        trace(vertexes, unusedEdges, usedEdges, edgeOrdinal, pw);
      }
      final int[] factors;
      if (edgeOrdinal == -1) {
        // No more edges. Are there any un-joined vertexes?
        final Vertex lastVertex = Util.last(vertexes);
        final int z = lastVertex.factors.previousClearBit(lastVertex.id - 1);
        if (z < 0) {
// PN: only time we exit from the for
          break;
        }
        factors = new int[] {z, lastVertex.id};
      } else {
        final LoptMultiJoin2.Edge bestEdge = unusedEdges.get(edgeOrdinal);

        // For now, assume that the edge is between precisely two factors.
        // 1-factor conditions have probably been pushed down,
        // and 3-or-more-factor conditions are advanced. (TODO:)
        // Therefore, for now, the factors that are merged are exactly the
        // factors on this edge.
        assert bestEdge.factors.cardinality() == 2;
        factors = bestEdge.factors.toArray();
      }
/// Let RL do the stuff above this loop. After this, the next steps
///should probably be the same?

      // Determine which factor is to be on the LHS of the join.
      final int majorFactor;
      final int minorFactor;
      if (vertexes.get(factors[0]).cost <= vertexes.get(factors[1]).cost) {
        majorFactor = factors[0];
        minorFactor = factors[1];
      } else {
        majorFactor = factors[1];
        minorFactor = factors[0];
      }
/// aren't these ever removed from vertexes?
/// maybe because we are adding the appropriate edges to usedEdges, then
/// we might not need to remove?
      final Vertex majorVertex = vertexes.get(majorFactor);
      final Vertex minorVertex = vertexes.get(minorFactor);


      // Find the join conditions. All conditions whose factors are now all in
      // the join can now be used.
/// FIXME: what does .set(v) do?
      final int v = vertexes.size();
      final ImmutableBitSet newFactors =
          majorVertex.factors
              .rebuild()
              .addAll(minorVertex.factors)
              .set(v)
              .build();

      final List<RexNode> conditions = new ArrayList<>();
      final Iterator<LoptMultiJoin2.Edge> edgeIterator = unusedEdges.iterator();
/// Note: based on the edge we chose, we might be removing a few other
/// edges from the graph as well.
      while (edgeIterator.hasNext()) {
        LoptMultiJoin2.Edge edge = edgeIterator.next();
        if (newFactors.contains(edge.factors)) {
          conditions.add(edge.condition);
          edgeIterator.remove();
          usedEdges.add(edge);
        }
      }

/// cost that we will send back to the RL agent?
/// TODO: experiment with using other cost formulas here.
      double cost =
          majorVertex.cost
          * minorVertex.cost
          * RelMdUtil.guessSelectivity(
              RexUtil.composeConjunction(rexBuilder, conditions, false));
      final Vertex newVertex =
          new JoinVertex(v, majorFactor, minorFactor, newFactors,
              cost, ImmutableList.copyOf(conditions));
      vertexes.add(newVertex);

/// PN: we might not care for this since we are letting RL handle it?

      // Re-compute selectivity of edges above the one just chosen.
      // Suppose that we just chose the edge between "product" (10k rows) and
      // "product_class" (10 rows).
      // Both of those vertices are now replaced by a new vertex "P-PC".
      // This vertex has fewer rows (1k rows) -- a fact that is critical to
      // decisions made later. (Hence "greedy" algorithm not "simple".)
      // The adjacent edges are modified.
      final ImmutableBitSet merged =
          ImmutableBitSet.of(minorFactor, majorFactor);
      for (int i = 0; i < unusedEdges.size(); i++) {
        final LoptMultiJoin2.Edge edge = unusedEdges.get(i);
        if (edge.factors.intersects(merged)) {
          ImmutableBitSet newEdgeFactors =
              edge.factors
                  .rebuild()
                  .removeAll(newFactors)
                  .set(v)
                  .build();
          assert newEdgeFactors.cardinality() == 2;
          final LoptMultiJoin2.Edge newEdge =
              new LoptMultiJoin2.Edge(edge.condition, newEdgeFactors,
                  edge.columns);
          unusedEdges.set(i, newEdge);
        }
      }
    }

/// we could have also started constructing the partial RelNode's while the
/// optimization above was going on --> would let us use
/// node.computeSelfCost. But probably can get that functionality some other
/// way too.

    // We have a winner!
    List<Pair<RelNode, TargetMapping>> relNodes = new ArrayList<>();
    for (Vertex vertex : vertexes) {
      if (vertex instanceof LeafVertex) {
/// don't need to make a join decision here I guess?
        LeafVertex leafVertex = (LeafVertex) vertex;
        final Mappings.TargetMapping mapping =
            Mappings.offsetSource(
                Mappings.createIdentity(
                    leafVertex.rel.getRowType().getFieldCount()),
                leafVertex.fieldOffset,
                multiJoin.getNumTotalFields());
        relNodes.add(Pair.of(leafVertex.rel, mapping));
      } else {
        JoinVertex joinVertex = (JoinVertex) vertex;
        final Pair<RelNode, Mappings.TargetMapping> leftPair =
            relNodes.get(joinVertex.leftFactor);
        RelNode left = leftPair.left;
        final Mappings.TargetMapping leftMapping = leftPair.right;
        final Pair<RelNode, Mappings.TargetMapping> rightPair =
            relNodes.get(joinVertex.rightFactor);
        RelNode right = rightPair.left;
        final Mappings.TargetMapping rightMapping = rightPair.right;
        final Mappings.TargetMapping mapping =
            Mappings.merge(leftMapping,
                Mappings.offsetTarget(rightMapping,
                    left.getRowType().getFieldCount()));
        if (pw != null) {
          pw.println("left: " + leftMapping);
          pw.println("right: " + rightMapping);
          pw.println("combined: " + mapping);
          pw.println();
        }
/// shuttle, woot?
        final RexVisitor<RexNode> shuttle =
            new RexPermuteInputsShuttle(mapping, left, right);
        final RexNode condition =
            RexUtil.composeConjunction(rexBuilder, joinVertex.conditions,
                false);
        final RelNode join = relBuilder.push(left)
            .push(right)
            .join(JoinRelType.INNER, condition.accept(shuttle))
            .build();
        relNodes.add(Pair.of(join, mapping));
      }
      if (pw != null) {
        pw.println(Util.last(relNodes));
      }
    }

/// just adding a projection on top of the left nodes we had.
    final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
    relBuilder.push(top.left)
        .project(relBuilder.fields(top.right));
    RelNode optNode = relBuilder.build();
    //System.out.println("bushy optimized node: " + RelOptUtil.toString(optNode));
    call.transformTo(optNode);
  }

  private void trace(List<Vertex> vertexes,
      List<LoptMultiJoin2.Edge> unusedEdges, List<LoptMultiJoin2.Edge> usedEdges,
      int edgeOrdinal, PrintWriter pw) {
    pw.println("bestEdge: " + edgeOrdinal);
    pw.println("vertexes:");
    for (Vertex vertex : vertexes) {
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

  int chooseBestEdge(List<LoptMultiJoin2.Edge> edges,
      Comparator<LoptMultiJoin2.Edge> comparator) {
    return minPos(edges, comparator);
  }

  /** Returns the index within a list at which compares least according to a
   * comparator.
   *
   * <p>In the case of a tie, returns the earliest such element.</p>
   *
   * <p>If the list is empty, returns -1.</p>
   */
  static <E> int minPos(List<E> list, Comparator<E> fn) {
    if (list.isEmpty()) {
      return -1;
    }
    E eBest = list.get(0);
    int iBest = 0;
    for (int i = 1; i < list.size(); i++) {
      E e = list.get(i);
      if (fn.compare(e, eBest) < 0) {
        eBest = e;
        iBest = i;
      }
    }
    return iBest;
  }

  /** Participant in a join (relation or join). */
  abstract static class Vertex {
    final int id;

    protected final ImmutableBitSet factors;
    final double cost;

    Vertex(int id, ImmutableBitSet factors, double cost) {
      this.id = id;
      this.factors = factors;
      this.cost = cost;
    }
  }

  /** Relation participating in a join. */
  static class LeafVertex extends Vertex {
    private final RelNode rel;
    final int fieldOffset;

    LeafVertex(int id, RelNode rel, double cost, int fieldOffset) {
      super(id, ImmutableBitSet.of(id), cost);
      this.rel = rel;
      this.fieldOffset = fieldOffset;
    }

    @Override public String toString() {
      return "LeafVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", fieldOffset: " + fieldOffset
          + ")";
    }
  }

  /** Participant in a join which is itself a join. */
  static class JoinVertex extends Vertex {
    private final int leftFactor;
    private final int rightFactor;
    /** Zero or more join conditions. All are in terms of the original input
     * columns (not in terms of the outputs of left and right input factors). */
    final ImmutableList<RexNode> conditions;

    JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet factors,
        double cost, ImmutableList<RexNode> conditions) {
      super(id, factors, cost);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.conditions = Objects.requireNonNull(conditions);
    }

    @Override public String toString() {
      return "JoinVertex(id: " + id
          + ", cost: " + Util.human(cost)
          + ", factors: " + factors
          + ", leftFactor: " + leftFactor
          + ", rightFactor: " + rightFactor
          + ")";
    }
  }
}

// End RLJoinOrderRule.java

