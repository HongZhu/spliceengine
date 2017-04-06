/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.kvpair.KVPair;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 10/16/15.
 */
public class IndexTransformFunction <Op extends SpliceOperation> extends SpliceFunction<Op,LocatedRow,KVPair> {
    private boolean initialized;
    private DDLMessage.TentativeIndex tentativeIndex;
    private int[] projectedMapping;

    private transient IndexTransformer transformer;

    public IndexTransformFunction() {
        super();
    }

    public IndexTransformFunction(DDLMessage.TentativeIndex tentativeIndex) {
        this.tentativeIndex = tentativeIndex;
        List<Integer> actualList = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        List<Integer> sortedList = new ArrayList<>(actualList);
        Collections.sort(sortedList);
        projectedMapping = new int[sortedList.size()];
        for (int i =0; i<projectedMapping.length;i++) {
            projectedMapping[i] = sortedList.indexOf(actualList.get(i));
        }

    }

    @Override
    public KVPair call(LocatedRow locatedRow) throws Exception {
        if (!initialized)
            init();
        ExecRow misMatchedRow = locatedRow.getRow();
        ExecRow row = new ValueRow(misMatchedRow.nColumns());
        for (int i = 0; i<projectedMapping.length;i++) {
              row.setColumn(i+1,misMatchedRow.getColumn(projectedMapping[i]+1));
        }
        locatedRow.setRow(row);
        return transformer.writeDirectIndex(locatedRow);
    }

    private void init() {
        transformer = new IndexTransformer(tentativeIndex);
        initialized = true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        byte[] message = tentativeIndex.toByteArray();
        out.writeInt(message.length);
        out.write(message);
        ArrayUtil.writeIntArray(out,projectedMapping);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte[] message = new byte[in.readInt()];
        in.readFully(message);
        tentativeIndex = DDLMessage.TentativeIndex.parseFrom(message);
        projectedMapping= ArrayUtil.readIntArray(in);
        initialized = false;
    }

    public long getIndexConglomerateId() {
        return tentativeIndex.getIndex().getConglomerate();
    }

    public List<Integer> getIndexColsToMainColMapList() {
        List<Integer> indexColsToMainColMapList = tentativeIndex.getIndex().getIndexColsToMainColMapList();
        return indexColsToMainColMapList;
    }
}
