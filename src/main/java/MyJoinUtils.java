//TODO: reduce num imports.
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/// other imports
import org.apache.calcite.tools.*;
import java.util.*;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;

public class MyJoinUtils {

  public static Program genJoinRule(
	  final Iterable<? extends RelOptRule> rules,
	  final int minJoinCount) {

	return (planner, rel, requiredOutputTraits, materializations, lattices) -> {

	  requiredOutputTraits.replace(EnumerableConvention.INSTANCE);
	  final int joinCount = RelOptUtil.countJoins(rel);
	  final Program program;
	  if (joinCount < minJoinCount) {
		System.out.println("too few joins!");
		// decide wha to do?
		program = Programs.ofRules(rules);
	  } else {
		// Create a program that gathers together joins as a MultiJoin.
		final HepProgram hep = new HepProgramBuilder()
			.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
			.addMatchOrder(HepMatchOrder.BOTTOM_UP)
			.addRuleInstance(JoinToMultiJoinRule.INSTANCE)
			.build();
		final Program program1 =
			Programs.of(hep, false, DefaultRelMetadataProvider.INSTANCE);

		// Create a program that contains a rule to expand a MultiJoin
		// into heuristically ordered joins.
		// We use the rule set passed in, but remove JoinCommuteRule and
		// JoinPushThroughJoinRule, because they cause exhaustive search.
		final List<RelOptRule> list = Lists.newArrayList(rules);
		//list.removeAll(
			//ImmutableList.of(JoinCommuteRule.INSTANCE,
				//JoinAssociateRule.INSTANCE,
				//JoinPushThroughJoinRule.LEFT,
				//JoinPushThroughJoinRule.RIGHT));

        // TODO: add joinRule that is passed in -- should be a RelOptRule.
        // and maybe not pass in the list of RelOptRules??
		//list.add(JoinOrderTest.INSTANCE);

		final Program program2 = Programs.ofRules(list);
		program = Programs.sequence(program1, program2);
	  }
	  return program.run(
		  planner, rel, requiredOutputTraits, materializations, lattices);
	};
  }
}
