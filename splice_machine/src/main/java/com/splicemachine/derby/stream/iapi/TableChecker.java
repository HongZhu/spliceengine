package com.splicemachine.derby.stream.iapi;

import java.util.List;

/**
 * Created by jyuan on 2/12/18.
 */
public interface TableChecker {
    List<String> checkTableAndIndex(PairDataSet table,
                                    PairDataSet index,
                                    String schemaName,
                                    String tableName,
                                    String indexName) throws Exception;
}
