import org.apache.calcite.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;

// FIXME: all copy-pasted
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

public class QueryGraphUtils {

  PrintWriter pw;
  public QueryGraphUtils(PrintWriter pw)
  {
    this.pw = pw;
  }

  /** Participant in a join (relation or join). */
  public static abstract class Vertex {
    final int id;

    protected final ImmutableBitSet factors;
    final double cost;
    // one hot features based on the DQ paper
    public ImmutableBitSet visibleFeatures;

    Vertex(int id, ImmutableBitSet factors, double cost) {
      this.id = id;
      this.factors = factors;
      this.cost = cost;
    }
  }

  public static class LeafVertex extends Vertex {
      public final RelNode rel;
      final int fieldOffset;

      LeafVertex(int id, RelNode rel, double cost, int fieldOffset)
      {
        super(id, ImmutableBitSet.of(id), cost);
        this.rel = rel;
        this.fieldOffset = fieldOffset;
        //initialize visibleFeatures with all the bits in the range turned on
        ImmutableBitSet.Builder visibleFeaturesBuilder = ImmutableBitSet.builder();
        int fieldCount = rel.getRowType().getFieldCount();
        for (int i = fieldOffset; i < fieldOffset+fieldCount; i++) {
          // TODO: decide if we should set this or not depending on the topmost
          // projection
          visibleFeaturesBuilder.set(i);
        }
        visibleFeatures = visibleFeaturesBuilder.build();
      }

      @Override public String toString() {
        return "LeafVertex(id: " + id
            + ", cost: " + Util.human(cost)
            + ", factors: " + factors
            + ", fieldOffset: " + fieldOffset
            + ", visibleFeatures: " + visibleFeatures
            + ")";
      }
    }

  public static class JoinVertex extends Vertex {
      public final int leftFactor;
      public final int rightFactor;
      final ImmutableList<RexNode> conditions;

      JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet factors,
          double cost, ImmutableList<RexNode> conditions, ImmutableBitSet visibleFeatures)      {
        super(id, factors, cost);
        this.leftFactor = leftFactor;
        this.rightFactor = rightFactor;
        this.conditions = Objects.requireNonNull(conditions);
        this.visibleFeatures = visibleFeatures;
      }

      @Override public String toString() {
        return "JoinVertex(id: " + id
            + ", cost: " + Util.human(cost)
            + ", factors: " + factors
            + ", leftFactor: " + leftFactor
            + ", rightFactor: " + rightFactor
            + ", visibleFeatures: " + visibleFeatures
            + ")";
      }
    }

  /*
   * After an edge is chosen for a join, this will collapse the vertices
   * connected by that edge into a single vertex. It will also update the edges
   * in the graph to reflect the new edges, and add a cost estimate for the new
   * vertex. This will be added to vertexes, but the old vertices will NOT be
   * removed.
   * @ret: the cost of the edge collapse.
   */
  public double updateGraph(List<Vertex> vertexes, int [] factors, List<LoptMultiJoin2.Edge> usedEdges, List<LoptMultiJoin2.Edge> unusedEdges, MyMetadataQuery mq, RexBuilder rexBuilder)
  {
    // FIXME: this control should be given to the RL agent.
    final int majorFactor;
    final int minorFactor;
    if (vertexes.get(factors[0]).cost <= vertexes.get(factors[1]).cost) {
      majorFactor = factors[0];
      minorFactor = factors[1];
    } else {
      majorFactor = factors[1];
      minorFactor = factors[0];
    }

    final Vertex majorVertex = vertexes.get(majorFactor);
    final Vertex minorVertex = vertexes.get(minorFactor);
    // set v ensures that the new vertex we are creating will be added to the
    // factors of the new vertex.
    final int v = vertexes.size();
    final ImmutableBitSet newFactors =
        majorVertex.factors
            .rebuild()
            .addAll(minorVertex.factors)
            .set(v)
            .build();

    final ImmutableBitSet newFeatures =
        majorVertex.visibleFeatures
            .rebuild()
            .addAll(minorVertex.visibleFeatures)
            .build();

    // Find the join conditions. All conditions whose factors are now all in
    // the join can now be used.
    final List<RexNode> conditions = new ArrayList<>();
    final Iterator<LoptMultiJoin2.Edge> edgeIterator = unusedEdges.iterator();
    while (edgeIterator.hasNext()) {
      LoptMultiJoin2.Edge edge = edgeIterator.next();
      if (newFactors.contains(edge.factors)) {
        conditions.add(edge.condition);
        edgeIterator.remove();
        usedEdges.add(edge);
      }
    }

    double cost =
        majorVertex.cost
        * minorVertex.cost
        * RelMdUtil.guessSelectivity(
            RexUtil.composeConjunction(rexBuilder, conditions, false));

    final Vertex newVertex = new JoinVertex(v, majorFactor, minorFactor,
        newFactors, cost, ImmutableList.copyOf(conditions), newFeatures);
    vertexes.add(newVertex);
    final ImmutableBitSet merged = ImmutableBitSet.of(minorFactor,
        majorFactor);

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
    return cost;
  }

  public void updateRelNodes(Vertex vertex, List<Pair<RelNode, TargetMapping>>
      relNodes, RexBuilder rexBuilder, RelBuilder relBuilder, LoptMultiJoin
      multiJoin)
  {
    if (vertex instanceof LeafVertex) {
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
      // TODO: what is the use of this shuttle + mappings here?
      // TODO: examine condition before / after the modifications to see
      // whats up. Seems to be making sure that the ids are mapped to the
      // correct ones.
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

}
