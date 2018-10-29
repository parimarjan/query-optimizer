import java.util.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.plan.hep.*;

public class MyUtils {

  public static String getTableName(RelNode rel) {
    if (rel == null) {
      return null;
    }
    if (rel instanceof RelSubset) {
      RelSubset s = (RelSubset) rel;
      return getTableName(s.getOriginal());
    } else if (rel instanceof Filter) {
      return getTableName(rel.getInput(0));
    } else if (rel instanceof HepRelVertex) {
      return getTableName(((HepRelVertex) rel).getCurrentRel());
    } else if (rel instanceof TableScan) {
      List<String> names = rel.getTable().getQualifiedName();
      if (names != null) {
        // TODO: is the more general version ever needed?
        //String tableName = "";
        //for (String s : names) {
          //tableName += s "-";
        //}
        //return tableName;
        return names.get(1);
      }
    }
    return null;
  }


}
