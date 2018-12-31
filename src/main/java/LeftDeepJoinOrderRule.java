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
public class LeftDeepJoinOrderRule extends RelOptRule {
  public static final LeftDeepJoinOrderRule INSTANCE =
      new LeftDeepJoinOrderRule(RelFactories.LOGICAL_BUILDER);

  private LoptMultiJoin multiJoin;
  private MyMetadataQuery mq;
  private int numOptionsConsidered = 0;
  private HashMap<Set<Integer>, QueryGraph> memoizedBestJoins;

  /** Creates an LeftDeepJoinOrderRule. */
  public LeftDeepJoinOrderRule(RelBuilderFactory relBuilderFactory) {
    super(operand(MultiJoin.class, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public LeftDeepJoinOrderRule(RelFactories.JoinFactory joinFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(joinFactory, projectFactory));
  }

//http://www.mathcs.emory.edu/~cheung/Courses/554/Syllabus/5-query-opt/dyn-prog-join2.html
  @Override
  public void onMatch(RelOptRuleCall call)
  {
    // Setting original expressions importance to 0, so our choice will be
    // chosen.
    RelNode orig = call.getRelList().get(0);
    call.getPlanner().setImportance(orig, 0.0);
    memoizedBestJoins = new HashMap<Set<Integer>, QueryGraph>();

    final MultiJoin multiJoinRel = call.rel(0);
    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final MyMetadataQuery mq = MyMetadataQuery.instance();
    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);
    QueryGraph queryGraph = new QueryGraph(multiJoin, mq, rexBuilder, relBuilder);

    // k = 1 case.
    for (int i = 0; i < multiJoin.getNumJoinFactors(); i++) {

       //IntermediateJoinState curJoinState = new IntermediateJoinState(new ArrayList(vertexes), new ArrayList(usedEdges), new ArrayList(unusedEdges), 0.00, ((QueryGraphUtils.LeafVertex) vertexes.get(i)).rel);
       HashSet<Integer> tmpH = new HashSet<Integer>();
       tmpH.add(i);
       //memoizedBestJoins.put(tmpH, new queryGraph);
    }

    // optimization: consider ab = ba.
    //for (int k = 2; k < multiJoin.getNumJoinFactors()+1; k++) {
			//List<Set<Integer>> res = new ArrayList<>();
      //getSubsets(initialVertexIdxs, k, 0, new HashSet<Integer>(), res);
      //for (Set<Integer> subset : res) {
        ////System.out.println("subset: " + subset);
        //// the subset might already be memoized.
        //double minCost = 10e10;
        //IntermediateJoinState bestJoinState = null;

        //// basically, at the end of this iteration, we should have the current
        //// subset entered into the memoized map.
        //for (Integer r : subset) {
          //HashSet<Integer> S_i = new HashSet<Integer>(subset);
          //S_i.remove(r);
          ////System.out.println("S_i is: " + S_i);
          //IntermediateJoinState curJoinState = memoizedBestJoins.get(S_i);
          //if (curJoinState == null) {
            ////System.out.println("curJoinState == null");
            //continue;
          //}

          //// try to do the join represented by curJoinState AND the factor r.
          //// Can only do this join if there is an edge connecting r with one of
          //// the elements in S_i. This must be one of the unusedEdges.
          //List<QueryGraphUtils.Vertex> curVertexes = new ArrayList(curJoinState.curVertexes);
          //List<LoptMultiJoin2.Edge> curUsedEdges = new ArrayList(curJoinState.curUsedEdges);
          //List<LoptMultiJoin2.Edge> curUnusedEdges = new ArrayList(curJoinState.curUnusedEdges);
          //double curCost = curJoinState.curCost;
          //ImmutableBitSet.Builder neededFactorsBld = ImmutableBitSet.builder();
          //neededFactorsBld.set(r);
          //if (S_i.size() == 1) {
            //for (Integer s_i : S_i) {
              //neededFactorsBld.set(s_i);
            //}
          //} else {
            //// the last vertex in curVertexes must be the right factor
            //neededFactorsBld.set(curVertexes.size() - 1);
          //}

          //ImmutableBitSet neededFactors = neededFactorsBld.build();

          //// FIXME: this might not work if edge.factors have the latest
          //// vertex rather than original indices of all the factors.
          //// for vertices with more than one factor, we know the last vertex
          //// added to curVertexes represents the best joined vertex so far
          //// (...)
          //Integer bestEdge = null;
          //for (int edgeOrd = 0; edgeOrd < curUnusedEdges.size(); edgeOrd+=1) {
            //LoptMultiJoin2.Edge curEdge = curUnusedEdges.get(edgeOrd);
            //if (curEdge.factors.equals(neededFactors)) {
              //bestEdge = edgeOrd;
              //break;
            //}
          //}
          //if (bestEdge == null) {
            ////System.out.println("no best edge!!!");
            //continue;
          //}
          ////System.out.println("found best edge: " + bestEdge);

          //final LoptMultiJoin2.Edge curEdge = curUnusedEdges.get(bestEdge);
          //assert curEdge.factors.cardinality() == 2;
          //int [] factors = curEdge.factors.toArray();

          //curCost += qGraphUtils.updateGraph(curVertexes, factors, curUsedEdges,
              //curUnusedEdges, mq, rexBuilder);

          //if (curCost < minCost) {
            //minCost = curCost;
       ////IntermediateJoinState curJoinState = new IntermediateJoinState(new ArrayList(vertexes), new ArrayList(usedEdges), new ArrayList(unusedEdges), 0.00, ((QueryGraphUtils.LeafVertex) vertexes.get(i)).rel);
            //// FIXME: do these need a new?
            ////bestJoinState = new IntermediateJoinState(curVertexes, curUsedEdges, curUnusedEdges, curCost, null);
            //bestJoinState = new IntermediateJoinState(new ArrayList(curVertexes), new ArrayList(curUsedEdges), new ArrayList(curUnusedEdges), curCost, null);

          //}
        //}

        //if (bestJoinState == null) continue;
        ////IntermediateJoinState curJoinState = memoizedBestJoins.get(S_i);
        //memoizedBestJoins.put(subset, bestJoinState);
      //}
    //}
    //Set<Integer> allFactors = new HashSet<Integer>(initialVertexIdxs);
    //IntermediateJoinState bestState = memoizedBestJoins.get(allFactors);
    //RelBuilder relBuilder = call.builder();

    //// celebrate and add the relNode being developed so far from optRelNodes
    //// to the set of equivalent nodes.
    //List<Pair<RelNode, TargetMapping>> optRelNodes = new ArrayList<>();
    //for (QueryGraphUtils.Vertex v : bestState.curVertexes) {
      //qGraphUtils.updateRelNodes(v, optRelNodes, rexBuilder, relBuilder, multiJoin);
    //}
    //final Pair<RelNode, Mappings.TargetMapping> top = Util.last(optRelNodes);
    //if (top == null) return;
    //relBuilder.push(top.left)
        //.project(relBuilder.fields(top.right));
    //RelNode optNode = relBuilder.build();
    //double trueCost = ((MyCost) mq.getCumulativeCost(optNode)).getCost();

    //call.transformTo(optNode);
  }

//https://stackoverflow.com/questions/12548312/find-all-subsets-of-length-k-in-an-array
  private static void getSubsets(List<Integer> superSet, int k, int idx, Set<Integer> current,List<Set<Integer>> solution) {
    //successful stop clause
    if (current.size() == k) {
        solution.add(new HashSet<>(current));
        return;
    }
    //unseccessful stop clause
    if (idx == superSet.size()) return;
    Integer x = superSet.get(idx);
    current.add(x);
    //"guess" x is in the subset
    getSubsets(superSet, k, idx+1, current, solution);
    current.remove(x);
    //"guess" x is not in the subset
    getSubsets(superSet, k, idx+1, current, solution);
	}

}

// End LeftDeepJoinOrderRule.java

