package com.splicemachine.si.constants;

import com.splicemachine.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Created by jleach on 12/9/15.
 */
@SuppressFBWarnings("MS_MUTABLE_ARRAY")
public class SIConstants {
    public static final int TRANSACTION_TABLE_BUCKET_COUNT = 16; //must be a power of 2
    public static final byte[] TRUE_BYTES = Bytes.toBytes(true);
    public static final byte[] FALSE_BYTES = Bytes.toBytes(false);
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] SNAPSHOT_ISOLATION_FAILED_TIMESTAMP = new byte[] {-1};
    public static final int TRANSACTION_START_TIMESTAMP_COLUMN = 0;
    public static final int TRANSACTION_PARENT_COLUMN = 1;
    public static final int TRANSACTION_DEPENDENT_COLUMN = 2;
    public static final int TRANSACTION_ALLOW_WRITES_COLUMN = 3;
    public static final int TRANSACTION_READ_UNCOMMITTED_COLUMN = 4;
    public static final int TRANSACTION_READ_COMMITTED_COLUMN = 5;
    public static final int TRANSACTION_STATUS_COLUMN = 6;
    public static final int TRANSACTION_COMMIT_TIMESTAMP_COLUMN = 7;
    public static final int TRANSACTION_KEEP_ALIVE_COLUMN = 8;
    public static final int TRANSACTION_ID_COLUMN = 14;
    public static final int TRANSACTION_COUNTER_COLUMN = 15;
    public static final int TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN = 16;

    /**
     * Splice Columns
     *
     * 0 = contains commit timestamp (optionally written after writing transaction is final)
     * 1 = tombstone (if value empty) or anti-tombstone (if value "0")
     * 7 = encoded user data
     * 9 = column for causing write conflicts between concurrent transactions writing to parent and child FK tables
     */
    public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes("0");
    public static final byte[] SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES = Bytes.toBytes("1");
    public static final byte[] SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES = Bytes.toBytes("9");
    public static final byte[] SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES = Bytes.toBytes("0");


    public static final String SI_TRANSACTION_KEY = "T";
    public static final String SI_TRANSACTION_ID_KEY = "A";
    public static final String SI_NEEDED = "B";
    public static final String SI_DELETE_PUT = "D";
    public static final String SI_COUNT_STAR = "M";

    //common SI fields
    public static final String NA_TRANSACTION_ID = "NA_TRANSACTION_ID";
    public static final String SI_EXEMPT = "si-exempt";

    public static final byte[] SI_NEEDED_VALUE_BYTES = Bytes.toBytes((short) 0);

    // The column in which splice stores encoded/packed user data.
    public static final byte[] PACKED_COLUMN_BYTES = Bytes.toBytes("7");

    public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes("V");

    public static final String SI_PERMISSION_FAMILY = "P";

    // Default Constants
    public static final String SUPPRESS_INDEXING_ATTRIBUTE_NAME = "iu";
    public static final byte[] SUPPRESS_INDEXING_ATTRIBUTE_VALUE = new byte[]{};
    public static final String CHECK_BLOOM_ATTRIBUTE_NAME = "cb";

    public static final String ENTRY_PREDICATE_LABEL= "p";

}
