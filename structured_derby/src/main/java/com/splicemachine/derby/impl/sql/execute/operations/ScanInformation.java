package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Represents metadata around Scanning operations. One implementation will delegate down to Derby,
 * another to some other method, depending on the shape of the implementation.
 *
 * @author Scott Fines
 * Created on: 10/1/13
 */
interface ScanInformation {

    void initialize(SpliceOperationContext opContext);

    ExecRow getResultRow() throws StandardException;

    boolean isKeyed() throws StandardException;

    FormatableBitSet getAccessedColumns() throws StandardException;

    Scan getScan(String txnId) throws StandardException;

    Qualifier[][] getScanQualifiers() throws StandardException;

    public String printStartPosition(int numOpens) throws StandardException;

    public String printStopPosition(int numOpens) throws StandardException;

    long getConglomerateId();
}
