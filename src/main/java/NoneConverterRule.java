import java.util.*;
import org.apache.calcite.plan.*;
import com.google.common.collect.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.adapter.jdbc.*;
import org.apache.calcite.plan.volcano.RelSubset;

import java.io.PrintWriter;
import org.apache.calcite.sql.*;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeField;

public class NoneConverterRule extends ConverterRule {
    public static final NoneConverterRule INSTANCE = new NoneConverterRule();

    public NoneConverterRule() {
      super(
          RelNode.class, Convention.NONE, EnumerableConvention.INSTANCE,
          "NoneConverterRule");
    }

    @Override public RelNode convert(RelNode rel) {
        System.out.println("rel instance: " + rel.getClass().getName());
        System.out.println("rel convention: " + rel.getConvention());
        System.out.println("rel query: " + rel.getQuery());
        if (rel instanceof RelSubset) {
            RelSubset subs = (RelSubset) rel;
            System.out.println("best: " + subs.getBest());
            for (RelNode rel2 : subs.getRels()) {
                System.out.println("rel2 instance: " + rel2.getClass().getName());
                System.out.println("rel2 convention: " + rel2.getConvention());
                System.out.println("rel2 query: " + rel2.getQuery());
            }
        }

        //if (rel instanceof LogicalJoin) {
        if (false) {
            LogicalJoin join = (LogicalJoin) rel;
            RelOptCluster cluster = join.getCluster();
            System.out.println("logical join!!!!");
            //RelTraitSet traitSet = RelTraitSet.createEmpty().replace(EnumerableConvention.INSTANCE);
            RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
            //RelTraitSet traitSet = cluster.traitSetOf(new JdbcConvention(null, null, "myjdbc"));
            //ImmutableList<RelDataTypeField> systemFieldList = (ImmutableList<RelDataTypeField>) ImmutableList.of(join.getSystemFieldList());
            System.out.println("leaving after converting the node!");
            return new LogicalJoin(cluster, traitSet, join.getLeft(), join.getRight(), join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(), (ImmutableList<RelDataTypeField>) join.getSystemFieldList());
        }
        return rel;
    }
}

