package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.utils.IntArrays;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by jyuan on 2/5/18.
 */
public class CheckTableJob implements Callable<Void> {

    public static final int MAX_ERROR = 10000;
    private final OlapStatus jobStatus;
    private final DistributedCheckTableJob request;
    private String tableName;
    private String schemaName;
    private Activation activation;
    private TxnView txn;
    private DataDictionary dd;
    private List<DDLMessage.TentativeIndex> tentativeIndexList;
    private long heapConglomId;
    private TableDescriptor td;
    private ConglomerateDescriptorList cdList;
    private DDLMessage.Table table;
    LanguageConnectionContext lcc;
    private String tableVersion;

    public CheckTableJob(DistributedCheckTableJob request,OlapStatus jobStatus) {
        this.jobStatus = jobStatus;
        this.request = request;
    }

    @Override
    public Void call() throws Exception {
        if(!jobStatus.markRunning()){
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }
        init();

        String table = Long.toString(heapConglomId);
        Collection<PartitionLoad> partitionLoadCollection = EngineDriver.driver().partitionLoadWatcher().tableLoad(table,false);
        boolean distributed = false;
        for (PartitionLoad load: partitionLoadCollection) {
            if (load.getMemStoreSizeMB() > 0 || load.getStorefileSizeMB() > 0)
                distributed = true;
        }
        DataSetProcessor dsp = null;
        if (distributed)
            dsp =EngineDriver.driver().processorFactory().distributedProcessor();
        else
            dsp = EngineDriver.driver().processorFactory().localProcessor(null,null);

        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.jobGroup, "");

        CheckTableResult checkTableResult = new CheckTableResult();
        Map<String, List<String>> errors = new HashMap<>();

        PairDataSet<String, ExecRow> tableDataSet = getTableDataSet(dsp, heapConglomId);
        TableChecker tableChecker = dsp.getTableChecker();

        for(DDLMessage.TentativeIndex tentativeIndex : tentativeIndexList) {
            DDLMessage.Index index = tentativeIndex.getIndex();
            String indexName = getIndexName(cdList, index.getConglomerate());
            PairDataSet<String, ExecRow> indexDataSet = getIndexDataSet(dsp, index.getConglomerate(), index.getUnique());

            List<String> messages = tableChecker.checkTableAndIndex(tableDataSet,
                    indexDataSet, schemaName, tableName, indexName);
            if (messages.size() > 0) {
                errors.put(indexName, messages);
            }
        }

        if (errors.size() > 0) {
            checkTableResult.setResults(errors);
        }

        jobStatus.markCompleted(checkTableResult);
        return null;
    }

    private PairDataSet<String, ExecRow> getIndexDataSet(DataSetProcessor dsp, long conglomerateId, boolean isUnique) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();
        int[] columnOrdering = IntArrays.count(isUnique? formatIds.length - 1 :formatIds.length);
        int[] baseColumnMap = IntArrays.count(formatIds.length);
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(formatIds.length);
        for (int i = 0; i < formatIds.length; ++i) {
            accessedKeyColumns.set(i);
        }
        ExecRow templateRow = new ValueRow(formatIds.length);
        DataValueDescriptor[] dvds = templateRow.getRowArray();
        DataValueFactory dataValueFactory=lcc.getDataValueFactory();
        for(int i=0;i<formatIds.length; i++){
            dvds[i] = dataValueFactory.getNull(formatIds[i],-1);
        }

        DataSet<ExecRow> scanSet =
                dsp.<TableScanOperation,ExecRow>newScanSet(null, Long.toString(conglomerateId))
                        .activation(activation)
                        .tableDisplayName(tableName)
                        .transaction(txn)
                        .scan(createScan(txn))
                        .tableVersion(tableVersion)
                        .template(templateRow)
                        .keyColumnEncodingOrder(columnOrdering)
                        .keyColumnTypes(ScanOperation.getKeyFormatIds(columnOrdering, formatIds))
                        .accessedKeyColumns(accessedKeyColumns)
                        .keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .rowDecodingMap(ScanOperation.getRowDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .baseColumnMap(columnOrdering)
                        .buildDataSet();

        PairDataSet<String, ExecRow> dataSet = scanSet.keyBy(new KeyByBaseRowIdFunction());

        return dataSet;
    }

    private PairDataSet<String, ExecRow> getTableDataSet(DataSetProcessor dsp, long conglomerateId) throws StandardException {
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(conglomerateId);
        int[] formatIds = conglomerate.getFormat_ids();

        int[] columnOrdering = conglomerate.getColumnOrdering();
        int maxCol = 0;
        for (int i = 0; i < columnOrdering.length; ++i) {
            if (columnOrdering[i] > maxCol)
                maxCol = columnOrdering[i];
        }
        int[] baseColumnMap = new int[maxCol+1];
        for (int i = 0; i < baseColumnMap.length; ++i) {
            baseColumnMap[i] = -1;
        }
        for (int i = 0; i < columnOrdering.length; ++i) {
            baseColumnMap[columnOrdering[i]] = i;
        }
        FormatableBitSet accessedKeyColumns = new FormatableBitSet(columnOrdering.length);
        for (int i = 0; i < columnOrdering.length; ++i) {
            accessedKeyColumns.set(i);
        }
        ExecRow templateRow = new ValueRow(columnOrdering.length);
        DataValueDescriptor[] dvds = templateRow.getRowArray();
        DataValueFactory dataValueFactory=lcc.getDataValueFactory();
        for(int i=0;i<columnOrdering.length; i++){
            dvds[i] = dataValueFactory.getNull(formatIds[columnOrdering[i]],-1);
        }

        DataSet<ExecRow> scanSet =
                dsp.<TableScanOperation,ExecRow>newScanSet(null, Long.toString(conglomerateId))
                        .activation(activation)
                        .tableDisplayName(tableName)
                        .transaction(txn)
                        .scan(createScan(txn))
                        .tableVersion(tableVersion)
                        .template(templateRow)
                        .keyColumnEncodingOrder(columnOrdering)
                        .keyColumnTypes(ScanOperation.getKeyFormatIds(columnOrdering, formatIds))
                        .accessedKeyColumns(accessedKeyColumns)
                        .keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .rowDecodingMap(ScanOperation.getRowDecodingMap(accessedKeyColumns, columnOrdering, baseColumnMap))
                        .baseColumnMap(baseColumnMap)
                        .buildDataSet();

        PairDataSet<String, ExecRow> dataSet = scanSet.keyBy(new KeyByRowIdFunction());

        return dataSet;
    }

    private void init() throws StandardException, SQLException{
        tentativeIndexList = request.tentativeIndexList;
        schemaName = request.schemaName;
        tableName = request.tableName;
        activation = request.ah.getActivation();
        txn = request.txn;
        table = tentativeIndexList.get(0).getTable();
        heapConglomId = table.getConglomerate();
        lcc = activation.getLanguageConnectionContext();
        SpliceTransactionManager tc = (SpliceTransactionManager)lcc.getTransactionExecute();
        dd =lcc.getDataDictionary();

        SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
        boolean prepared = false;
        try {
            prepared=transactionResource.marshallTransaction(txn);
            SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
            td = dd.getTableDescriptor(tableName, sd, tc);
            cdList = td.getConglomerateDescriptorList();
            tableVersion = td.getVersion();
        }
        finally {
            if (prepared)
                transactionResource.close();
        }
    }

    private DataScan createScan (TxnView txn) {
        DataScan scan= SIDriver.driver().getOperationFactory().newDataScan(txn);
        scan.returnAllVersions(); //make sure that we read all versions of the data
        return scan.startKey(new byte[0]).stopKey(new byte[0]);
    }

    private String getIndexName(ConglomerateDescriptorList cds, long conglomerateNumber) {
        for (int i = 0; i < cds.size(); ++i) {
            ConglomerateDescriptor cd = cds.get(i);
            if (cd.getConglomerateNumber() == conglomerateNumber) {
                return cd.getObjectName();
            }
        }
        return null;
    }
}
