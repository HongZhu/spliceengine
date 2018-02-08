package com.splicemachine.derby.stream.control;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.storage.CheckTableJob;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import org.apache.commons.collections.MultiMap;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 2/12/18.
 */
public class ControlTableChecker implements TableChecker {

    private String schemaName;
    private String tableName;
    private String indexName;
    private long tableCount = 0;
    private long indexCount = 0;
    private long invalidIndexCount = 0;
    private long missingIndexCount = 0;

    private ListMultimap<String, ExecRow> indexData;
    private Map<String, ExecRow> tableData;
    @Override
    public List<String> checkTableAndIndex(PairDataSet table,
                                           PairDataSet index,
                                           String schemaName,
                                           String tableName,
                                           String indexName) throws Exception {

        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        populateData(table, index);

        List<String> messages = Lists.newLinkedList();
        messages.addAll(checkInvalidIndexes(table, index));
        messages.addAll(checkMissingIndexes(table, index));
        if (indexCount - invalidIndexCount > tableCount - missingIndexCount) {
            messages.addAll(checkDuplicateIndexes(index));
        }
        return messages;
    }


    private void populateData(PairDataSet table, PairDataSet index) {
        if (tableData == null) {
            tableData = new HashMap<>();
            Iterator<Tuple2<String, ExecRow>> tableSource = ((ControlPairDataSet) table).source;
            while (tableSource.hasNext()) {
                tableCount++;
                Tuple2<String, ExecRow> t = tableSource.next();
                String rowId = t._1;
                ExecRow row = t._2;
                tableData.put(rowId, row);
            }
        }

        indexData = ArrayListMultimap.create();
        Iterator<Tuple2<String, ExecRow>> indexSource = ((ControlPairDataSet)index).source;
        while(indexSource.hasNext()) {
            indexCount++;
            Tuple2<String, ExecRow> t = indexSource.next();
            String baseRowId = t._1;
            ExecRow row = t._2;
            indexData.put(baseRowId, row);
        }
    }

    private List<String> checkDuplicateIndexes(PairDataSet index) {
        List<String> messages = Lists.newLinkedList();
        ArrayListMultimap<String, ExecRow> result = ArrayListMultimap.create();
        long duplicateIndexCount = 0;
        for (String baseRowId : indexData.keySet()) {
            List<ExecRow> rows = indexData.get(baseRowId);
            if (rows.size() > 1) {
                duplicateIndexCount += rows.size();
                result.putAll(baseRowId, rows);
            }
        }
        int i = 0;
        if (duplicateIndexCount > 0) {
            messages.add(String.format("The following %d indexes are duplicates:", duplicateIndexCount));
            for (String baseRowId : result.keySet()) {
                List<ExecRow> rows = result.get(baseRowId);
                for (ExecRow row : rows) {
                    if (i >= CheckTableJob.MAX_ERROR) {
                        messages.add("...");
                        break;
                    }
                    messages.add(row.toString());
                    i++;
                }
            }
        }

        return messages;
    }

    private List<String> checkInvalidIndexes(PairDataSet table, PairDataSet index) {
        List<String> messages = Lists.newLinkedList();
        ArrayListMultimap<String, ExecRow> result = ArrayListMultimap.create();
        for (String baseRowId : indexData.keySet()) {
            if (!tableData.containsKey(baseRowId)) {
                List<ExecRow> rows = indexData.get(baseRowId);
                result.putAll(baseRowId, rows);
                invalidIndexCount += rows.size();
            }
        }

        int i = 0;
        if (invalidIndexCount > 0) {
            messages.add(String.format("The following %d indexes are invalid:", invalidIndexCount));
            for (String baseRowId : result.keySet()) {
                List<ExecRow> rows = result.get(baseRowId);
                for (ExecRow row : rows) {
                    if (i >= CheckTableJob.MAX_ERROR) {
                        messages.add("...");
                        break;
                    }
                    messages.add(row.toString());
                    i++;
                }
            }
        }
        return  messages;
    }

    private List<String> checkMissingIndexes(PairDataSet table, PairDataSet index) {
        List<String> messages = Lists.newLinkedList();

        Map<String, ExecRow> result = new HashMap<>();
        for (String rowId : tableData.keySet()) {
            if (!indexData.containsKey(rowId)) {
                missingIndexCount++;
                result.put(rowId, tableData.get(rowId));
            }
        }

        int i = 0;
        if (missingIndexCount > 0) {
            messages.add(String.format("The following %d rows from base table %s.%s are not indexed:", result.size(), schemaName, tableName));
            for (Map.Entry<String, ExecRow> entry : result.entrySet()) {
                if (i >= CheckTableJob.MAX_ERROR) {
                    messages.add("...");
                    break;
                }
                ExecRow row = entry.getValue();
                if (row.nColumns() > 0) {
                    messages.add(entry.getValue().toString());
                }
                else {
                    messages.add(entry.getKey());
                }
                i++;
            }
        }
        return  messages;
    }
}
