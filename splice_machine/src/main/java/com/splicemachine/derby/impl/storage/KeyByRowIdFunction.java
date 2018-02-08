package com.splicemachine.derby.impl.storage;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFunction;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by jyuan on 2/6/18.
 */
public class KeyByRowIdFunction <Op extends SpliceOperation> extends SpliceFunction<Op,ExecRow, String> {

    @Override
    public String call(ExecRow row) throws Exception {
        return Bytes.toHex(row.getKey());
    }
}
