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
	// cumulative cost of the joins we have done so far in the QueryGraph.
	public Double costSoFar;
  // used to get the RelNode corresponding to the joins we have done so far.
  List<Pair<RelNode, TargetMapping>> relNodes = new ArrayList<>();

  // FIXME: maybe we can build this independently of this?
  private LoptMultiJoin multiJoin;
  private RexBuilder rexBuilder;
  private RelBuilder relBuilder;
  private MyMetadataQuery mq;
  private PrintWriter pw = null;
  private HashMap<Integer, Integer> mapToDatabase;

  /* TODO:
   */
	// FIXME: do we really need to use LoptMultiJoin? seems to have too many
	// unneccesary things. Perhaps we can just use MultiJoin / or have our own
	// class to represent the relevant fields?
  public QueryGraph(LoptMultiJoin multiJoin, MyMetadataQuery mq, RexBuilder rexBuilder, RelBuilder relBuilder)
  {
		this.costSoFar = 0.00;
    this.multiJoin = multiJoin;
    this.rexBuilder = rexBuilder;
    this.relBuilder = relBuilder;
    this.mq = mq;

    // set up the mapping to database offsets for each of our attributes
    HashMap<String, Integer> tableOffsets = DbInfo.getAllTableFeaturesOffsets();
    mapToDatabase = new HashMap<Integer, Integer>();
    int totalFieldCount = 0;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      RelNode rel = multiJoin.getJoinFactor(i);
      String tableName = MyUtils.getTableName(rel);
      Integer offset = tableOffsets.get(tableName);
      assert offset != null;
      int curFieldCount = rel.getRowType().getFieldCount();
      for (int j = 0; j < curFieldCount; j++) {
        mapToDatabase.put(j+totalFieldCount, offset+j);
      }
      totalFieldCount += curFieldCount;
    }

    allVertexes = new ArrayList<Vertex>();
    edges = new ArrayList<Edge>();
    // Add the orignal tables as vertexes
    int x = 0;
    String curQuery = QueryOptExperiment.getCurrentQuery();
    HashMap<String, Double> curQueryCard = mq.trueBaseCardinalities.get(curQuery);
    if (curQueryCard == null) {
      System.out.println("!!!!curQueryCard was null!!!!!");
      curQueryCard = new HashMap<String, Double>();
      mq.trueBaseCardinalities.put(curQuery, curQueryCard);
    }
    boolean cardinalitiesUpdated = false;
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {
      final RelNode rel = multiJoin.getJoinFactor(i);
      // this is a vertex, so must be one of the tables from the database
      String tableName = MyUtils.getTableName(rel);
      if (mq.trueBaseCardinalities.get(curQuery).get(tableName) == null) {
        if (tableName.equals("cast_info") || tableName.equals("movie_info")) {
          // if it is NOT a filter block
					if (RelOptUtil.toString(rel).contains("Filter")) {
            System.out.println("bad table filter!");
            Double trueCard = QueryOptExperiment.getTrueCardinality(rel);
            curQueryCard.put(tableName, trueCard);
					} else {
            if (tableName.equals("cast_info")) {
              curQueryCard.put(tableName, 36244344.00);
            } else if (tableName.equals("movie_info")) {
              curQueryCard.put(tableName, 14835720.00);
            }
		      }
        } else {
          Double trueCard = QueryOptExperiment.getTrueCardinality(rel);
          curQueryCard.put(tableName, trueCard);
        }
        cardinalitiesUpdated = true;
      }
      double rowCount = mq.getRowCount(rel);
      Vertex newVertex = new LeafVertex(i, rel, rowCount, x);
      allVertexes.add(newVertex);
      updateRelNodes(newVertex);
      x += rel.getRowType().getFieldCount();
    }
    if (cardinalitiesUpdated) {
      mq.saveUpdatedCardinalities();
    }

    // add the Edges
    for (RexNode node : multiJoin.getJoinFilters()) {
      edges.add(createEdge(node));
    }

    // TODO: maybe use this version?
    //final List<LoptMultiJoin2.Edge> unusedEdges = new ArrayList<>();
    //for (RexNode node : multiJoin.getJoinFilters()) {
      //LoptMultiJoin2.Edge edge = multiJoin.createEdge2(node);
      //boolean newEdge = true;
      //for (LoptMultiJoin2.Edge edge2 : unusedEdges) {
        //if (edge2.factors.contains(edge.factors)) {
          ////System.out.println("!!going to merge an edge!!");
          //newEdge = false;
          //// combine these edges
          //edge2.mergeEdge(edge, rexBuilder);
          //break;
        //}
      //}
      //if (newEdge) edges.add(edge);
    //}
  }

	/*
	 */
	// FIXME: this can not be called multiple times as it changes the state of
	// the relBuilder. But would be nice if we could call it multiple times.
	// Potentially, all we need to do is to undo the final changes we made to the
	// relBuilder (?)
	public RelNode getFinalOptimalRelNode() {
		final Pair<RelNode, Mappings.TargetMapping> top = Util.last(relNodes);
		relBuilder.push(top.left)
				.project(relBuilder.fields(top.right));
		RelNode optNode = relBuilder.build();
		return optNode;
	}

  // FIXME: should not need to be static, stop using this class outside.
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

  public class LeafVertex extends Vertex
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
      // right now, we have all possible attributes turned on
      visibleAttrs = getVisibleAttributes(visibleAttrs);
    }

    @Override public String toString()
    {
      return
        "{'id': " + id
        + ", 'estimated_cardinality': " + cost
        + ", 'factors': " + factors
        + ", 'visibleAttributes': " + visibleAttrs
        + "}";
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
      return "{'id': " + id
        + ", 'estimated_cardinality': " + cost
        + ", 'factors': " + factors
        + ", 'leftFactor': " + leftFactor
        + ", 'rightFactor': " + rightFactor
        + ", 'visibleAttributes': " + visibleAttrs
        + "}";
    }
  }

  /** Information about a join-condition. */
  public class Edge
  {
    public ImmutableBitSet factors;
    public ImmutableBitSet columns;
    RexNode condition;

    public Edge (RexNode condition, ImmutableBitSet factors, ImmutableBitSet
        columns)
    {
      this.condition = condition;
      this.factors = factors;
      // FIXME: do we need to check if columns order matches the order of
      // factors?
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
      return "{'factors': " +  factors
        + ", 'joinAttributes':" + columns
        + "}";
    }
  }

  /// FIXME: can we make this private?
  public ImmutableBitSet mapToDBFeatures(ImmutableBitSet bs)
  {
    ImmutableBitSet.Builder featuresBuilder = ImmutableBitSet.builder();
    for (Integer i : bs) {
      Integer test = mapToDatabase.get(i);
      if (test == null) {
        System.out.println("test was null!!!");
      }
      featuresBuilder.set(mapToDatabase.get(i));
    }
    return featuresBuilder.build();
  }

  /**
   * @allPossibleAttributes: bitset with all the attribute positions
   * corresponding to the node turned on. This is wrt only to the tables in the
   * current query.
   *
   * ret: bitset with all the attribute positions turned on that are USED in
   * this query, and with respect to ALL the tables in the dataset.
   */
  private ImmutableBitSet getVisibleAttributes(ImmutableBitSet
      allPossibleAttributes)
  {
    ImmutableBitSet queryAttributes = DbInfo.getCurrentQueryVisibleFeatures();
    ImmutableBitSet retAttributes = allPossibleAttributes.intersect(queryAttributes);
    retAttributes = mapToDBFeatures(retAttributes);
    return retAttributes;
  }

  /*
   * After an edge is chosen for a join, this will collapse the vertices
   * connected by that edge into a single vertex. It will also update the edges
   * in the graph to reflect the new edges, and add a cost estimate for the new
   * vertex. This will be added to vertexes, but the old vertices will NOT be
   * removed.
   * @ret: the cost of choosing this edge for the next join, as computed by the
   * metadataQueryProvider.
   */
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
      }
    }

		// FIXME: these variables need to be renamed to estRowCount
    double rowCount =
        majorVertex.cost
        * minorVertex.cost
        * RelMdUtil.guessSelectivity(
            RexUtil.composeConjunction(rexBuilder, conditions, false));

    final Vertex newVertex = new JoinVertex(v, majorFactor, minorFactor,
        newFactors, rowCount, ImmutableList.copyOf(conditions), newFeatures);
    allVertexes.add(newVertex);
    allVertexes.set(majorFactor, null);
    allVertexes.set(minorFactor, null);

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
    // we update the relNodes right away so we can use those to compute the
    // cost using our cost model
    updateRelNodes(newVertex);
    // use the last relNode that we just added
    Pair<RelNode, Mappings.TargetMapping> curTop = Util.last(relNodes);
    RelNode curOptNode = curTop.left;
    double cost = ((MyCost) mq.getNonCumulativeCost(curOptNode)).getCost();
		this.costSoFar += cost;
    return cost;
  }

  /* Updates the list of relNodes that correspond to each join order selection.
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
    // this is the first time that the edge has been created, so we map it's
    // fields to be wrt to the full DB rather than just the current query
    fieldRefBitmap = mapToDBFeatures(fieldRefBitmap);
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
