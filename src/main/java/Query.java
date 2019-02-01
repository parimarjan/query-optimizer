import java.util.*;
import java.io.*;
import org.apache.commons.io.FileUtils;
import com.google.gson.Gson;

public class Query {

  public String fileName;
  public String sql;

  // FIXME: maybe instead of having a million maps, we just have a queryPlanner
  // object which keeps track of all this??
  public HashMap<String, Double> costs;
  public HashMap<String, List<int[]>> joinOrders;
  public HashMap<String, Long> planningTimes;
  //public HashMap<String, String> plans;
  public HashMap<String, Integer> resultVerifier;

  // will represent postgres, mysql, monetdb runtimes
  HashMap<String, Long> dbmsRuntimes;
  // key will be the join order of the algorithm. Saving for each join order so
  // we can avoid re-running queries
  HashMap<List<Integer>, Double> RLRuntimes;

  // TODO: add alternative init methods
  public Query(File f) throws Exception {
    fileName = f.getName();
    sql = FileUtils.readFileToString(f);
    sql = queryRewriteForCalcite(sql);

    // initialize all the guys
    resultVerifier = new HashMap<String, Integer>();
    //plans = new HashMap<String, String>();
    joinOrders = new HashMap<String, List<int[]>>();
    costs = new HashMap<String, Double>();
    planningTimes = new HashMap<String, Long>();
    dbmsRuntimes = new HashMap<String, Long>();
  }

  public String toJson() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  private String queryRewriteForCalcite(String query) {
      String newQuery = query.replace(";", "");
      newQuery = newQuery.replace("!=", "<>");
      // weird trouble for calcite because of this
      newQuery = newQuery.replace("AS character", "");
      // using `at' for a table alias seems to cause trouble
      //newQuery = newQuery.replace("\\bat,\\b", "att,");
      newQuery = newQuery.replace("at,", "att,");
      newQuery = newQuery.replace("at.", "att.");
      // debugging purposes
      // FIXME: doesn't seem easy to add text here without running into
      // weird formatting issues (while it works just fine if we write the
      // same thing in the original queries)
      //newQuery = "\"explain\" " + newQuery;
      //newQuery = newQuery + " LIMIT 10";
      return newQuery;
  }

  public boolean verifyResults() {
    // need to check that all the values in res must be the same!
    Integer rl_val = resultVerifier.get("RL");
    for (Integer val : resultVerifier.values()) {
      if (!(val == rl_val)) {
        return false;
      }
    }
    return true;
  }
}
