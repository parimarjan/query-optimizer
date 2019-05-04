// FIXME: too many imports?
//import org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.RelOptUtil;

import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.volcano.*;
import org.apache.calcite.plan.hep.*;
import org.apache.calcite.adapter.jdbc.*;
import org.apache.calcite.rel.logical.*;
import java.util.*;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import java.util.concurrent.ThreadLocalRandom;

//import org.apache.calcite.rel.rel2sql;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
//import org.apache.calcite.rel.RelToSqlConverter;
//import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.rel.rel2sql.SqlImplementor.Result;
import org.apache.calcite.sql.SqlNode;
import java.util.Random;

public class MyMetadataQuery extends RelMetadataQuery {
  private class JsonCardinalities
  {
    Map<String, Map<String, Double>> cardinalities;
  }

  // in terms of number of rows. This is used for calculating the cost in a
  // non-linear model.
  private final double MEMORY_LIMIT = Math.pow(10, 6);
  // TODO: support options: CM1, CM2, CM3, RowCount
  private final String COST_MODEL_NAME;

  // FIXME: temporary solution. make this general purpose.
  private final String BASE_CARDINALITIES_FILE_NAME = "cardinalities.ser";
  public HashMap<String, HashMap<String, Double>> trueBaseCardinalities;
  private final String CARDINALITIES_FILE = "test.json";
  public HashMap<String, HashMap<String, Long>> cards;

  /**
   * Returns an instance of RelMetadataQuery. It ensures that cycles do not
   * occur while computing metadata.
   */
  public static MyMetadataQuery instance() {
    return new MyMetadataQuery(THREAD_PROVIDERS.get(), EMPTY);
  }

	protected MyMetadataQuery(JaninoRelMetadataProvider metadataProvider,
      RelMetadataQuery prototype) {
		super(metadataProvider, prototype);
    this.COST_MODEL_NAME = QueryOptExperiment.getCostModelName();
    trueBaseCardinalities = (HashMap) loadCardinalities();
    if (trueBaseCardinalities == null) {
      trueBaseCardinalities = new HashMap<String, HashMap<String, Double>>();
    }
    QueryOptExperiment.Params params = QueryOptExperiment.getParams();
    if (params.cardinalitiesModel.equals("file")) {
      File file = new File(CARDINALITIES_FILE);
      String jsonStr = null;
      try {
        jsonStr = FileUtils.readFileToString(file, "UTF-8");
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
      Gson gson = new Gson();
      cards = gson.fromJson(jsonStr, new TypeToken<HashMap<String, HashMap<String, Long>>>() {}.getType());
      //System.out.println(cards);
    }
  }

  @Override
  public Double getRowCount(RelNode rel) {
    // FIXME: more error checking needs to be done here!!!!
    QueryOptExperiment.Params params = QueryOptExperiment.getParams();
    Query query = QueryOptExperiment.getCurrentQuery();

    // trying to get sql query here FAILS because VolcanoPlanner's RelSubset
    // doesn't have a RelToSqlConverter implementation...
    //try {
      //if (true) {
				//if (rel instanceof RelSubset) {
					//// this seems like it should need special handling, but it probably wraps
					//// around either Filter / TableScan, so it will be handled when this
					//// function is called again.
					////System.out.println("case 4");
					//return super.getRowCount(rel);
				//} else {
          //System.out.println(rel);
					//RelToSqlConverter relToSqlConverter	= new RelToSqlConverter(AnsiSqlDialect.DEFAULT);
					//System.out.println("got rel2Sql!");
					//RelToSqlConverter.Result res = relToSqlConverter.visitChild(0, rel);
					//SqlNode sqlNode = res.asQueryOrValues();
					//String result = sqlNode.toSqlString(AnsiSqlDialect.DEFAULT, false).getSql();
					//System.out.println(result);
					////Result sqlres = relSql.visit(rel);
					////Result sqlres = relSql.result(rel);
					////System.out.println(sqlRes);
					////System.exit(-1);
				//}
		//}
    //} catch (Exception e) {
      //e.printStackTrace();
      ////System.exit(-1);
    //}
    // in this case, the provided cardinality file should have entries for
    // each of the needed queries.
    // TODO: explain the format better.
    ArrayList<String> tableNames = MyUtils.getAllTableNames(rel);
    Double rowCount = null;
    if (params.cardinalitiesModel.equals("file")) {
      java.util.Collections.sort(tableNames);
      String tableKey = "";
      for (String tN : tableNames) {
        tableKey += " " + tN;
      }
      //if (!tableKey.contains("null")) {
        //System.out.println("filename: " + query.fileName);
        //String fileName = "join-order-benchmark/" + query.fileName;
        //HashMap<String, Long> qCards = cards.get(fileName);
        //if (qCards == null) {
          //System.out.println("qCards is null!");
          //System.exit(-1);
        //} else {
          //Long rowCountLong = qCards.get(tableKey);
          ////System.out.println("found rowCount from file!: " + rowCount);
          //if (rowCountLong != null) {
            //rowCount = rowCountLong.doubleValue();
          //}
          ////System.out.println("row count was null!");
          ////System.out.println("fileName: " + query.fileName);
          ////System.out.println("tableKey: " + tableKey);
          ////System.exit(-1);
        //}
      //}
    }
    if (rowCount == null) {
      // Default: use true cardinalities for the base tables, and calcite's
      // default handling for all the joins (some sort of very simple Selinger
      // model...)
      String sqlQuery = query.sql;
      HashMap<String, Double> curQueryMap = trueBaseCardinalities.get(sqlQuery);
      if (curQueryMap == null) {
        //System.out.println("case 1");
        rowCount = super.getRowCount(rel);
      }
      if (rel instanceof Filter || rel instanceof TableScan) {
        String tableName = MyUtils.getTableName(rel);
        if (tableName == null) {
          rowCount = super.getRowCount(rel);
        } else {
          rowCount = curQueryMap.get(tableName);
        }
        if (rowCount == null) {
          //System.out.println("case 2");
          rowCount = super.getRowCount(rel);
        }
        //System.out.println("case 3");
      } else if (rel instanceof RelSubset) {
        // this seems like it should need special handling, but it probably wraps
        // around either Filter / TableScan, so it will be handled when this
        // function is called again.
        //System.out.println("case 4");
        rowCount = super.getRowCount(rel);
      }
      if (rowCount == null) {
        //System.out.println("case 5");
        rowCount = super.getRowCount(rel);
      }
    }

    // don't need to do anything if no error
    if (params.cardinalityError.equals("shiftedError")) {
      rowCount += 100.00;
    } else if (params.cardinalityError.equals("gaussianError")) {
      Random rng = new Random();
      rowCount += rng.nextGaussian() * params.cardErrorRange + params.cardErrorRange;
    } else if (params.cardinalityError.equals("sameDirectionError")) {
      Random rng = new Random();
      rowCount += Math.abs(rng.nextGaussian() * params.cardErrorRange
              + params.cardErrorRange);
    } else if (params.cardinalityError.equals("nearestPowerOfTwo")) {
      rowCount = Math.ceil(Math.log(rowCount)/Math.log(2));
    } else if (params.cardinalityError.equals("randomError")) {
      Random rng = new Random();
      rowCount = Math.abs(rng.nextGaussian() * params.cardErrorRange
              + params.cardErrorRange);
    } else if (params.cardinalityError.equals("nearest1000")) {
      rowCount = (Double) (Math.round(rowCount/1000.0) * 1000.0);
    } else if (params.cardinalityError.equals("nearest10000")) {
      rowCount = (Double) (Math.round(rowCount/10000.0) * 10000.0);
    } else if (params.cardinalityError.equals("nearest100000")) {
      rowCount = (Double) (Math.round(rowCount/100000.0) * 100000.0);
    } else if (params.cardinalityError.equals("uniformError")) {
      //Integer errRadius = params.cardErrorRange.integerValue();
      Integer errRadius = params.cardErrorRange / 2;
      rowCount += (Double)
        ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
    } else if (params.cardinalityError.equals("uniformNumTableError")) {
      int numTables = tableNames.size();
      Integer errRadius = params.cardErrorRange / 2;
      errRadius = errRadius*(numTables-1);
      rowCount += (Double)
        ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
    } else if (params.cardinalityError.equals("mixedError")) {
      int NUM_RANDS = 4;
      int randNum = ThreadLocalRandom.current().nextInt(0, NUM_RANDS);
      if (randNum % NUM_RANDS == 0) {
        // unifrom error.
        Integer errRadius = params.cardErrorRange / 2;
        rowCount += (Double)
          ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
      } else if (randNum % NUM_RANDS == 1) {
        // gaussian error
        Random rng = new Random();
        rowCount += Math.abs(rng.nextGaussian() * params.cardErrorRange
                + params.cardErrorRange);
      } else if (randNum % NUM_RANDS == 2) {
        // uniformNumTableError
        int numTables = tableNames.size();
        Integer errRadius = params.cardErrorRange / 2;
        errRadius = errRadius*(numTables-1);
        rowCount += (Double)
          ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
      } else if (randNum % NUM_RANDS == 3) {
        // leave same
      }
    } else if (params.cardinalityError.equals("uniformPercentError")) {
      Integer errRadius = params.cardErrorRange / 2;
      Double errPercentage = (Double)
        ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
      Double change = rowCount * (errPercentage / 100.00);
      rowCount += change;
    } else if (params.cardinalityError.equals("uniformMixedPercentError")) {
      int basePercent = params.cardErrorRange / 2;
      int randNum = ThreadLocalRandom.current().nextInt(1, 5);
      Integer errRadius = basePercent*randNum;
      Double errPercentage = (Double)
        ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
      Double change = rowCount * (errPercentage / 100.00);
      rowCount += change;
    } else if (params.cardinalityError.equals("uniformPercentTablesError")) {
      int numTables = tableNames.size();
      Integer errRadius = (params.cardErrorRange / 2)*numTables;
      Double errPercentage = (Double)
        ((Integer) ThreadLocalRandom.current().nextInt(-errRadius, errRadius + 1)).doubleValue();
      Double change = rowCount * (errPercentage / 100.00);
      rowCount += change;
    }
    return rowCount;
  }

	@Override
  public RelOptCost getCumulativeCost(RelNode rel) {
    if (COST_MODEL_NAME.equals("rowCount")) {
      // FIXME: is this actually just rowCount w/o any modifications?
      RelOptCost origCost = super.getCumulativeCost(rel);
      return origCost;
    }

    if (rel instanceof Join) {
      RelNode left = ((Join) rel).getLeft();
      RelNode right = ((Join) rel).getRight();
      RelOptCost leftCost = getCumulativeCost(left);
      RelOptCost rightCost = getCumulativeCost(right);
      RelOptCost curCost = getNonCumulativeCost(rel);

      if (COST_MODEL_NAME.equals("CM2")) {
        return leftCost.plus(rightCost).plus(curCost);
      } else {
        // FIXME:
        System.err.println("unsupported cost model " + COST_MODEL_NAME);
        System.exit(-1);
      }
    }
    // else just treat it as usual
    RelOptCost origCost = super.getCumulativeCost(rel);
    return origCost;
  }

	@Override
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    RelOptCost orig_cost = super.getNonCumulativeCost(rel);
    if (rel instanceof Join && !COST_MODEL_NAME.equals("rowCount")) {
      RelNode left = ((Join) rel).getLeft();
      RelNode right = ((Join) rel).getRight();
      double leftRows = getRowCount(left);
      double rightRows = getRowCount(right);
      double curRows = getRowCount(rel);
      if (COST_MODEL_NAME.equals("CM2")) {
        // Now, the cost model will consider whether the leftRows and rightRows
        // fit in the memory or not.
        if ((leftRows + rightRows) < MEMORY_LIMIT) {
          return orig_cost;
        } else if (Math.min(leftRows, rightRows) < MEMORY_LIMIT) {
          //System.out.println("!!!case 2!!!");
          double newCost = 2*(leftRows + rightRows) + curRows;
          ((MyCost) orig_cost).cost = newCost;
        } else {
          double newCost = rightRows + Math.ceil(rightRows / MEMORY_LIMIT) * leftRows + curRows;
          ((MyCost) orig_cost).cost = newCost;
        }
      }
    }
    return orig_cost;
  }

  public void saveUpdatedCardinalities() {
    HashMap<String, HashMap<String, Double>> oldCosts = (HashMap) loadCardinalities();
    HashMap<String, HashMap<String, Double>> newCosts = new HashMap<String, HashMap<String, Double>>();
    if (oldCosts != null){
      // ignore this guy, file probably didn't exist.
      newCosts.putAll(oldCosts);
    }
    newCosts.putAll(trueBaseCardinalities);
    saveCardinalities(newCosts);
  }

  // FIXME: make these general purpose
  private void saveCardinalities(Serializable obj)
  {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(
							new FileOutputStream(BASE_CARDINALITIES_FILE_NAME)
			);
			oos.writeObject(obj);
			oos.flush();
			oos.close();
		} catch (Exception e) {
			System.out.println(e);
		}
  }

  public Serializable loadCardinalities() {
    try {
      FileInputStream fis = new FileInputStream(BASE_CARDINALITIES_FILE_NAME);
      ObjectInputStream ois = new ObjectInputStream(fis);
      HashMap<String, HashMap<String, Double>> cards = (HashMap) ois.readObject();
      ois.close();
      return cards;
    } catch (Exception e) {
      System.out.println(e);
    }
    return null;
  }
}

