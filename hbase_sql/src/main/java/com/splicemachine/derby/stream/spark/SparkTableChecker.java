package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.storage.CheckTableJob;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 2/12/18.
 */
public class SparkTableChecker implements TableChecker {

    private long tableCount = 0;
    private long indexCount = 0;
    private long invalidIndexCount = 0;
    private long missingIndexCount = 0;
    private String schemaName;
    private String tableName;
    private String indexName;

    @Override
    public List<String> checkTableAndIndex(PairDataSet table,
                                           PairDataSet index,
                                           String schemaName,
                                           String tableName,
                                           String indexName) throws Exception {

        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;

        List<String> messages = Lists.newLinkedList();

        JavaFutureAction<Long> tableCountFuture = null;
        if (tableCount == 0) {
            SpliceSpark.pushScope(String.format("Count table %s.%s", schemaName, tableName));
            tableCountFuture = ((SparkPairDataSet) table).rdd.countAsync();
            SpliceSpark.popScope();
        }
        SpliceSpark.pushScope(String.format("Count index %s.%s", schemaName, indexName));
        JavaFutureAction<Long> indexCountFuture = ((SparkPairDataSet)index).rdd.countAsync();
        SpliceSpark.popScope();

        messages.addAll(checkInvalidIndexes(table, index));
        messages.addAll(checkMissingIndexes(table, index));

        if (tableCountFuture != null) {
            tableCount = tableCountFuture.get();
        }

        indexCount = indexCountFuture.get();

        if (indexCount - invalidIndexCount > tableCount - missingIndexCount) {
            messages.addAll(checkDuplicateIndexes(index));
        }
        return messages;
    }

    private List<String> checkDuplicateIndexes(PairDataSet index) {
        List<String> messages = Lists.newLinkedList();
        SpliceSpark.pushScope(String.format("Check duplicates in index %s.%s", schemaName, indexName));
        JavaPairRDD duplicateIndexRdd = ((SparkPairDataSet)index).rdd
                .combineByKey(new createCombiner(), new mergeValue(), new mergeCombiners())
                .filter(new filter());

        Map<String, List<ExecRow>> duplicateIndexes = duplicateIndexRdd.collectAsMap();

        int i = 0;
        for (List<ExecRow> rows : duplicateIndexes.values()) {
            for (ExecRow row : rows) {

                if (i >= CheckTableJob.MAX_ERROR) {
                    messages.add("...");
                    break;
                }
                messages.add(row.toString());
                i++;
            }
        }
        messages.add(0, String.format("The following %d indexes are duplicates:", i));

        SpliceSpark.popScope();
        return messages;
    }


    private List<String> checkInvalidIndexes(PairDataSet table, PairDataSet index) {
        List<String> messages = Lists.newLinkedList();

        SpliceSpark.pushScope(String.format("Check invalidate index from %s.%s", schemaName, indexName));
        PairDataSet<String, ExecRow> d1 = index.subtractByKey(table, null);
        Map<String, ExecRow> result = d1.collectAsMap();
        SpliceSpark.popScope();
        invalidIndexCount = result.size();
        int i = 0;
        if (invalidIndexCount > 0) {
            messages.add(String.format("The following %d indexes are invalid:", result.size()));
            for (Map.Entry<String, ExecRow> entry : result.entrySet()) {
                if (i >= CheckTableJob.MAX_ERROR) {
                    messages.add("...");
                    break;
                }
                messages.add(entry.getValue().toString());
                i++;
            }
        }
        return  messages;
    }


    private List<String> checkMissingIndexes(PairDataSet table, PairDataSet index) {
        List<String> messages = Lists.newLinkedList();
        SpliceSpark.pushScope(String.format("Check unindexed rows from table %s.%s", schemaName, tableName));
        PairDataSet<String, ExecRow> d2 = table.subtractByKey(index, null);
        Map<String, ExecRow> result = d2.collectAsMap();
        SpliceSpark.popScope();
        missingIndexCount = result.size();
        int i = 0;
        if (missingIndexCount > 0) {
            messages.add(String.format("The following %d rows from base table %s.%s are not indexed:", result.size(), schemaName, tableName));
            for (Map.Entry<String, ExecRow> entry : result.entrySet()) {
                ExecRow row = entry.getValue();
                if (row.nColumns() > 0) {
                    messages.add(entry.getValue().toString());
                }
                else {
                    messages.add(entry.getKey());
                }
                if (i >= CheckTableJob.MAX_ERROR) {
                    messages.add("...");
                    break;
                }
                i++;
            }
        }
        return  messages;
    }

    public static class createCombiner implements Function<ExecRow, List<ExecRow>> {

        @Override
        public List<ExecRow> call(ExecRow row) {

            List<ExecRow> l = Lists.newLinkedList();
            l.add(row);
            return l;
        }
    }


    public static class mergeValue implements Function2<List<ExecRow>, ExecRow, List<ExecRow>> {

        @Override
        public List<ExecRow> call(List<ExecRow> l1, ExecRow row) {

            l1.add(row);
            return l1;
        }
    }
    public static class mergeCombiners implements Function2<List<ExecRow>, List<ExecRow>, List<ExecRow>> {

        @Override
        public List<ExecRow> call(List<ExecRow> l1, List<ExecRow> l2) {

            l1.addAll(l2);
            return l1;
        }
    }

    public static class filter implements Function<scala.Tuple2<String, List<ExecRow>>,Boolean> {

        @Override
        public Boolean call(scala.Tuple2<String, List<ExecRow>> tuple2) {
            return tuple2._2.size() > 1;
        }
    }
}
