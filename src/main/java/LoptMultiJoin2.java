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
//package org.apache.calcite.rel.rules;

import org.apache.calcite.rel.rules.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * Utility class that keeps track of the join factors that
 * make up a {@link MultiJoin}.
 */
public class LoptMultiJoin2 extends LoptMultiJoin {

  // maps the field id's within the current query (0....num_fields in the
  // tables accessed by the current query) to the id's of these fields in all
  // the tables in the databse.
  HashMap<Integer, Integer> mapToDatabase;
  public LoptMultiJoin2(MultiJoin mj) {
    super(mj);
    HashMap<String, Integer> tableOffsets = DbInfo.getAllTableFeaturesOffsets();
    mapToDatabase = new HashMap<Integer, Integer>();
    int totalFieldCount = 0;
    for (int i = 0; i < getNumJoinFactors(); i++) {
      RelNode rel = getJoinFactor(i);
      String tableName = MyUtils.getTableName(rel);
      Integer offset = tableOffsets.get(tableName);
      assert offset != null;
      int curFieldCount = rel.getRowType().getFieldCount();
      for (int j = 0; j < curFieldCount; j++) {
        mapToDatabase.put(j+totalFieldCount, offset+j);
      }
      totalFieldCount += curFieldCount;
    }
  }

	// functions needed because in LoptMultiJoin the Edges were private.
  // FIXME: find a simpler way to deal with this.

  public Edge createEdge2(RexNode condition) {
    ImmutableBitSet fieldRefBitmap = fieldBitmap(condition);
    ImmutableBitSet factorRefBitmap = factorBitmap(fieldRefBitmap);
    return new Edge(condition, factorRefBitmap, fieldRefBitmap);
  }

  private ImmutableBitSet fieldBitmap(RexNode joinFilter) {
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder();
    joinFilter.accept(inputFinder);
    return inputFinder.inputBitSet.build();
  }

  private ImmutableBitSet factorBitmap(ImmutableBitSet fieldRefBitmap) {
    ImmutableBitSet.Builder factorRefBitmap = ImmutableBitSet.builder();
    for (int field : fieldRefBitmap) {
      int factor = findRef(field);
      factorRefBitmap.set(factor);
    }
    return factorRefBitmap.build();
  }

	/** Information about a join-condition. */
  static class Edge {
    final ImmutableBitSet factors;
    final ImmutableBitSet columns;
    final RexNode condition;

    Edge (RexNode condition, ImmutableBitSet factors, ImmutableBitSet columns) {
      this.condition = condition;
      this.factors = factors;
      this.columns = columns;
    }

    @Override public String toString() {
      return "Edge(condition: " + condition
          + ", factors: " + factors
          + ", columns: " + columns + ")";
    }
  }
}
// End LoptMultiJoin2.java


