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

/* TODO:
 */
public class QueryGraph {

  // Note: these include all the vertexes in the collapsing query
  // graph as successive joins are chosen
  public ArrayList<Vertex> allVertexes;
  // These are only the edges in the CURRENT state of the query graph. Thus, to
  // get the current graph, just traverse the edges and add the appropriate
  // vertices
  public ArrayList<Edge> edges;
  // used to get the RelNode corresponding to the joins we have done so far.
  List<Pair<RelNode, TargetMapping>> relNodes = new ArrayList<>();

  // FIXME: maybe we can build this independently of this?
  // FIXME: make private
  public LoptMultiJoin2 multiJoin;
  private RexBuilder rexBuilder;
  private RelBuilder relBuilder;
  private MyMetadataQuery mq;
  private PrintWriter pw = null;

  /* TODO:
   */
  public QueryGraph(LoptMultiJoin2 multiJoin, MyMetadataQuery mq, RexBuilder rexBuilder, RelBuilder relBuilder)
  {
    this.multiJoin = multiJoin;
    this.rexBuilder = rexBuilder;
    this.relBuilder = relBuilder;
    this.mq = mq;

    allVertexes = new ArrayList<Vertex>();
    edges = new ArrayList<Edge>();
    // Add the orignal tables as vertexes
    int x = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      // this is a vertex, so must be one of the tables from the database
      double cost = mq.getRowCount(rel);
      Vertex newVertex = new LeafVertex(i, rel, cost, x);
      allVertexes.add(newVertex);
      // TODO: support this?
      updateRelNodes(newVertex);
      x += rel.getRowType().getFieldCount();
    }
    assert x == multiJoin.getNumTotalFields();
    // add the Edges
    for (RexNode node : multiJoin.getJoinFilters()) {
      edges.add(createEdge(node));
    }
  }

  /** Participant in a join (relation or join). */
  public static abstract class Vertex
  {
    final int id;

    protected final ImmutableBitSet factors;
    final double cost;
    // one hot attributes mapping based on the DQ paper
    public ImmutableBitSet visibleAttrs;

    Vertex(int id, ImmutableBitSet factors, double cost)
    {
      this.id = id;
      this.factors = factors;
      // FIXME: switch this to cardinality? Or just remove this?
      this.cost = cost;
    }
  }

  public static class LeafVertex extends Vertex
  {
    public final RelNode rel;
    final int fieldOffset;

    LeafVertex(int id, RelNode rel, double cost, int fieldOffset)
    {
      super(id, ImmutableBitSet.of(id), cost);
      this.rel = rel;
      this.fieldOffset = fieldOffset;
      // FIXME: need to map this to only visible bits in the query
      //initialize visibleAttrs with all the bits in the range turned on
      ImmutableBitSet.Builder visibleAttrsBuilder = ImmutableBitSet.builder();
      int fieldCount = rel.getRowType().getFieldCount();
      for (int i = fieldOffset; i < fieldOffset+fieldCount; i++) {
        visibleAttrsBuilder.set(i);
      }
      visibleAttrs = visibleAttrsBuilder.build();
    }

    @Override public String toString()
    {
      return "LeafVertex(id: " + id
        + ", cost: " + Util.human(cost)
        + ", factors: " + factors
        + ", fieldOffset: " + fieldOffset
        + ", visibleAttrs: " + visibleAttrs
        + ")";
    }
  }

  public static class JoinVertex extends Vertex
  {
    public final int leftFactor;
    public final int rightFactor;
    final ImmutableList<RexNode> conditions;

    public JoinVertex(int id, int leftFactor, int rightFactor, ImmutableBitSet
        factors, double cost, ImmutableList<RexNode> conditions,
        ImmutableBitSet visibleAttrs)
    {
      super(id, factors, cost);
      this.leftFactor = leftFactor;
      this.rightFactor = rightFactor;
      this.conditions = Objects.requireNonNull(conditions);
      this.visibleAttrs = visibleAttrs;
    }

    @Override
    public String toString() {
      return "JoinVertex(id: " + id
        + ", cost: " + Util.human(cost)
        + ", factors: " + factors
        + ", leftFactor: " + leftFactor
        + ", rightFactor: " + rightFactor
        + ", visibleAttrs: " + visibleAttrs
        + ")";
    }
  }

  /** Information about a join-condition. */
  public static class Edge
  {
    public ImmutableBitSet factors;
    public ImmutableBitSet columns;
    RexNode condition;

    public Edge (RexNode condition, ImmutableBitSet factors, ImmutableBitSet
        columns)
    {
      this.condition = condition;
      this.factors = factors;
      this.columns = columns;
    }

    // FIXME: verify everything works as expected.
    public void mergeEdge(Edge newEdge, RexBuilder rexBuilder)
    {
      assert newEdge.factors.contains(factors);
      // factors should already be updated. Now, update columns.
      ImmutableBitSet.Builder columnBuilder = ImmutableBitSet.builder();
      columnBuilder.addAll(this.columns);
      columnBuilder.addAll(newEdge.columns);
      columns = columnBuilder.build();
      // update conditions
      List<RexNode> conditions = new ArrayList<>();
      conditions.add(condition);
      conditions.add(newEdge.condition);
      condition = RexUtil.composeConjunction(rexBuilder, conditions, false);
    }

    @Override
    public String toString()
    {
    return "Edge(condition: " + condition
      + ", factors: " + factors
      + ", columns: " + columns + ")";
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
  // Changes: adds a vertex, and removes an edge.
  //
  public double updateGraph(int [] factors)
  {
    // FIXME: this control should be given to the RL agent.
    final int majorFactor;
    final int minorFactor;
    if (allVertexes.get(factors[0]).cost <= allVertexes.get(factors[1]).cost) {
      majorFactor = factors[0];
      minorFactor = factors[1];
    } else {
      majorFactor = factors[1];
      minorFactor = factors[0];
    }

    final Vertex majorVertex = allVertexes.get(majorFactor);
    final Vertex minorVertex = allVertexes.get(minorFactor);
    // set v ensures that the new vertex we are creating will be added to the
    // factors of the new vertex.
    final int v = allVertexes.size();
    final ImmutableBitSet newFactors = majorVertex.factors
            .rebuild()
            .addAll(minorVertex.factors)
            .set(v)
            .build();

    final ImmutableBitSet newFeatures =
        majorVertex.visibleAttrs
            .rebuild()
            .addAll(minorVertex.visibleAttrs)
            .build();

    // Find the join conditions. All conditions whose factors are now all in
    // the join can now be used.
    final List<RexNode> conditions = new ArrayList<>();
    final Iterator<Edge> edgeIterator = edges.iterator();
    while (edgeIterator.hasNext()) {
      Edge edge = edgeIterator.next();
      if (newFactors.contains(edge.factors)) {
        conditions.add(edge.condition);
        edgeIterator.remove();
        //usedEdges.add(edge);
      }
    }

    double cost =
        majorVertex.cost
        * minorVertex.cost
        * RelMdUtil.guessSelectivity(
            RexUtil.composeConjunction(rexBuilder, conditions, false));

    final Vertex newVertex = new JoinVertex(v, majorFactor, minorFactor,
        newFactors, cost, ImmutableList.copyOf(conditions), newFeatures);
    allVertexes.add(newVertex);
    final ImmutableBitSet merged = ImmutableBitSet.of(minorFactor,
        majorFactor);

    for (int i = 0; i < edges.size(); i++) {
      Edge edge = edges.get(i);
      if (edge.factors.intersects(merged)) {
        ImmutableBitSet newEdgeFactors =
            edge.factors
                .rebuild()
                .removeAll(newFactors)
                .set(v)
                .build();
        assert newEdgeFactors.cardinality() == 2;
        Edge newEdge = new Edge(edge.condition, newEdgeFactors, edge.columns);
        edges.set(i, newEdge);
      }
    }
    updateRelNodes(newVertex);
    return cost;
  }

  /* Updates the list of relNodes that correspond to each join order selection.
   * TODO: maybe we can simplify this?
   */
  private void updateRelNodes(Vertex vertex)
  {
    if (vertex instanceof LeafVertex) {
      LeafVertex leafVertex = (LeafVertex) vertex;
      final Mappings.TargetMapping mapping =
          // FIXME: Seems to fail sometimes here!!
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

  public Edge createEdge(RexNode condition)
  {
    ImmutableBitSet fieldRefBitmap = fieldBitmap(condition);
    ImmutableBitSet factorRefBitmap = factorBitmap(fieldRefBitmap);
    return new Edge(condition, factorRefBitmap, fieldRefBitmap);
  }

  private ImmutableBitSet fieldBitmap(RexNode joinFilter)
  {
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder();
    joinFilter.accept(inputFinder);
    return inputFinder.inputBitSet.build();
  }

  private ImmutableBitSet factorBitmap(ImmutableBitSet fieldRefBitmap)
  {
    ImmutableBitSet.Builder factorRefBitmap = ImmutableBitSet.builder();
    for (int field : fieldRefBitmap) {
      int factor = multiJoin.findRef(field);
      factorRefBitmap.set(factor);
    }
    return factorRefBitmap.build();
  }

}
