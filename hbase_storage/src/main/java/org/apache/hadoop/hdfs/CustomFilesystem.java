/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package org.apache.hadoop.hdfs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BlockStorageLocation;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class CustomFilesystem extends DistributedFileSystem {

    private final DistributedFileSystem delegate;
    //    private DistributedFileSystem super;
    private Connection connection;

    public CustomFilesystem(DistributedFileSystem fs, String connectionURL) throws IOException {
        delegate = fs;
        try {
            connection = DriverManager.getConnection(connectionURL);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getScheme() {
        return super.getScheme();
    }

    @Override
    public URI getUri() {
        return super.getUri();
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        this.dfs.close();
        this.dfs = new CustomDFSClient(uri, conf, statistics, connection);
    }

    @Override
    public Path getWorkingDirectory() {
        return super.getWorkingDirectory();
    }

    @Override
    public long getDefaultBlockSize() {
        return super.getDefaultBlockSize();
    }

    @Override
    public short getDefaultReplication() {
        return super.getDefaultReplication();
    }

    @Override
    public void setWorkingDirectory(Path dir) {
        super.setWorkingDirectory(dir);
    }

    @Override
    public Path getHomeDirectory() {
        return super.getHomeDirectory();
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, file.getPath().toString());
                statement.setString(2, "tokens");
                try (ResultSet rs = statement.executeQuery()) {
                    List<HdfsProtos.LocatedBlockProto> protos = new ArrayList<>();
                    while (rs.next()) {
                        Blob blob = rs.getBlob(1);
                        byte[] bytes = blob.getBytes(1, (int) blob.length());
                        HdfsProtos.LocatedBlockProto lbp = HdfsProtos.LocatedBlockProto.parseFrom(bytes);
                        protos.add(lbp);
                    }
                    return DFSUtil.locatedBlocks2Locations(PBHelper.convertLocatedBlock(protos));
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
        return getFileBlockLocations(getFileStatus(p), start, len);
    }

    @Override
    @InterfaceStability.Unstable
    @Deprecated
    public BlockStorageLocation[] getFileBlockStorageLocations(List<BlockLocation> blocks) throws IOException, UnsupportedOperationException, InvalidBlockTokenException {
        return super.getFileBlockStorageLocations(blocks);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
        super.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public boolean recoverLease(Path f) throws IOException {
        return super.recoverLease(f);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return super.open(f, bufferSize);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        return super.append(f, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public HdfsDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress, InetSocketAddress[] favoredNodes) throws IOException {
        return super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress, favoredNodes);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> cflags, int bufferSize, short replication, long blockSize, Progressable progress, Options.ChecksumOpt checksumOpt) throws IOException {
        return super.create(f, permission, cflags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flag, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return super.createNonRecursive(f, permission, flag, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean setReplication(Path src, short replication) throws IOException {
        return super.setReplication(src, replication);
    }

    @Override
    public void setStoragePolicy(Path src, String policyName) throws IOException {
        super.setStoragePolicy(src, policyName);
    }

    @Override
    public BlockStoragePolicy[] getStoragePolicies() throws IOException {
        return super.getStoragePolicies();
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        super.concat(trg, psrcs);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return super.rename(src, dst);
    }

    @Override
    public void rename(Path src, Path dst, Options.Rename... options) throws IOException {
        super.rename(src, dst, options);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return super.delete(f, recursive);
    }

    @Override
    public ContentSummary getContentSummary(Path f) throws IOException {
        return super.getContentSummary(f);
    }

    @Override
    public void setQuota(Path src, long namespaceQuota, long diskspaceQuota) throws IOException {
        super.setQuota(src, namespaceQuota, diskspaceQuota);
    }

    @Override
    public FileStatus[] listStatus(Path p) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, p.toUri().getPath());
                statement.setString(2, "list");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for getFileStatus");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    List<FileStatus> results = new ArrayList<>();
                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                    try {
                        while (dis.available() > 0) {
                            FileStatus status = new FileStatus();
                            status.readFields(dis);
                            results.add(status);
                        }
                    } finally {
                        dis.close();
                    }
                    FileStatus[] result = new FileStatus[results.size()];
                    int i = 0;
                    for (FileStatus fs : results) {
                        result[i++] = fs;
                    }
                    return result;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public RemoteIterator<FileStatus> listStatusIterator(Path p) throws IOException {
        return super.listStatusIterator(p);
    }

    @Override
    public boolean mkdir(Path f, FsPermission permission) throws IOException {
        return super.mkdir(f, permission);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return super.mkdirs(f, permission);
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            connection.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    @InterfaceAudience.Private
    @VisibleForTesting
    public DFSClient getClient() {
        return super.getClient();
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return super.getStatus(p);
    }

    @Override
    @Deprecated
    public DiskStatus getDiskStatus() throws IOException {
        return super.getDiskStatus();
    }

    @Override
    @Deprecated
    public long getRawCapacity() throws IOException {
        return super.getRawCapacity();
    }

    @Override
    @Deprecated
    public long getRawUsed() throws IOException {
        return super.getRawUsed();
    }

    @Override
    public long getMissingBlocksCount() throws IOException {
        return super.getMissingBlocksCount();
    }

    @Override
    public long getMissingReplOneBlocksCount() throws IOException {
        return super.getMissingReplOneBlocksCount();
    }

    @Override
    public long getUnderReplicatedBlocksCount() throws IOException {
        return super.getUnderReplicatedBlocksCount();
    }

    @Override
    public long getCorruptBlocksCount() throws IOException {
        return super.getCorruptBlocksCount();
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException {
        return super.listCorruptFileBlocks(path);
    }

    @Override
    public DatanodeInfo[] getDataNodeStats() throws IOException {
        return super.getDataNodeStats();
    }

    @Override
    public DatanodeInfo[] getDataNodeStats(HdfsConstants.DatanodeReportType type) throws IOException {
        return super.getDataNodeStats(type);
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action) throws IOException {
        return super.setSafeMode(action);
    }

    @Override
    public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked) throws IOException {
        return super.setSafeMode(action, isChecked);
    }

    @Override
    public void saveNamespace() throws AccessControlException, IOException {
        super.saveNamespace();
    }

    @Override
    public long rollEdits() throws AccessControlException, IOException {
        return super.rollEdits();
    }

    @Override
    public boolean restoreFailedStorage(String arg) throws AccessControlException, IOException {
        return super.restoreFailedStorage(arg);
    }

    @Override
    public void refreshNodes() throws IOException {
        super.refreshNodes();
    }

    @Override
    public void finalizeUpgrade() throws IOException {
        super.finalizeUpgrade();
    }

    @Override
    public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action) throws IOException {
        return super.rollingUpgrade(action);
    }

    @Override
    public void metaSave(String pathname) throws IOException {
        super.metaSave(pathname);
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return super.getServerDefaults();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, f.toUri().getPath());
                statement.setString(2, "status");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for getFileStatus");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    FileStatus status = new FileStatus();
                    Writables.copyWritable(bytes, status);
                    return status;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent) throws AccessControlException, FileAlreadyExistsException, FileNotFoundException, ParentNotDirectoryException, UnsupportedFileSystemException, IOException {
        super.createSymlink(target, link, createParent);
    }

    @Override
    public boolean supportsSymlinks() {
        return super.supportsSymlinks();
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException {
        return super.getFileLinkStatus(f);
    }

    @Override
    public Path getLinkTarget(Path f) throws AccessControlException, FileNotFoundException, UnsupportedFileSystemException, IOException {
        return super.getLinkTarget(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        return super.getFileChecksum(f);
    }

    @Override
    public FileChecksum getFileChecksum(Path f, long length) throws IOException {
        return super.getFileChecksum(f, length);
    }

    @Override
    public void setPermission(Path p, FsPermission permission) throws IOException {
        super.setPermission(p, permission);
    }

    @Override
    public void setOwner(Path p, String username, String groupname) throws IOException {
        super.setOwner(p, username, groupname);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        super.setTimes(p, mtime, atime);
    }

    @Override
    public Token<DelegationTokenIdentifier> getDelegationToken(String renewer) throws IOException {
        return super.getDelegationToken(renewer);
    }

    @Override
    public void setBalancerBandwidth(long bandwidth) throws IOException {
        super.setBalancerBandwidth(bandwidth);
    }

    @Override
    public String getCanonicalServiceName() {
        return super.getCanonicalServiceName();
    }
    @Override
    public boolean isInSafeMode() throws IOException {
        return super.isInSafeMode();
    }

    @Override
    public void allowSnapshot(Path path) throws IOException {
        super.allowSnapshot(path);
    }

    @Override
    public void disallowSnapshot(Path path) throws IOException {
        super.disallowSnapshot(path);
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName) throws IOException {
        return super.createSnapshot(path, snapshotName);
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName, String snapshotNewName) throws IOException {
        super.renameSnapshot(path, snapshotOldName, snapshotNewName);
    }

    @Override
    public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
        return super.getSnapshottableDirListing();
    }

    @Override
    public void deleteSnapshot(Path snapshotDir, String snapshotName) throws IOException {
        super.deleteSnapshot(snapshotDir, snapshotName);
    }

    @Override
    public SnapshotDiffReport getSnapshotDiffReport(Path snapshotDir, String fromSnapshot, String toSnapshot) throws IOException {
        return super.getSnapshotDiffReport(snapshotDir, fromSnapshot, toSnapshot);
    }

    @Override
    public boolean isFileClosed(Path src) throws IOException {
        return super.isFileClosed(src);
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo info) throws IOException {
        return super.addCacheDirective(info);
    }

    @Override
    public long addCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
        return super.addCacheDirective(info, flags);
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo info) throws IOException {
        super.modifyCacheDirective(info);
    }

    @Override
    public void modifyCacheDirective(CacheDirectiveInfo info, EnumSet<CacheFlag> flags) throws IOException {
        super.modifyCacheDirective(info, flags);
    }

    @Override
    public void removeCacheDirective(long id) throws IOException {
        super.removeCacheDirective(id);
    }

    @Override
    public RemoteIterator<CacheDirectiveEntry> listCacheDirectives(CacheDirectiveInfo filter) throws IOException {
        return super.listCacheDirectives(filter);
    }

    @Override
    public void addCachePool(CachePoolInfo info) throws IOException {
        super.addCachePool(info);
    }

    @Override
    public void modifyCachePool(CachePoolInfo info) throws IOException {
        super.modifyCachePool(info);
    }

    @Override
    public void removeCachePool(String poolName) throws IOException {
        super.removeCachePool(poolName);
    }

    @Override
    public RemoteIterator<CachePoolEntry> listCachePools() throws IOException {
        return super.listCachePools();
    }

    @Override
    public void modifyAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        super.modifyAclEntries(path, aclSpec);
    }

    @Override
    public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
        super.removeAclEntries(path, aclSpec);
    }

    @Override
    public void removeDefaultAcl(Path path) throws IOException {
        super.removeDefaultAcl(path);
    }

    @Override
    public void removeAcl(Path path) throws IOException {
        super.removeAcl(path);
    }

    @Override
    public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
        super.setAcl(path, aclSpec);
    }

    @Override
    public AclStatus getAclStatus(Path path) throws IOException {
        return super.getAclStatus(path);
    }

    @Override
    public void createEncryptionZone(Path path, String keyName) throws IOException {
        super.createEncryptionZone(path, keyName);
    }

    @Override
    public EncryptionZone getEZForPath(Path path) throws IOException {
        return super.getEZForPath(path);
    }

    @Override
    public RemoteIterator<EncryptionZone> listEncryptionZones() throws IOException {
        return super.listEncryptionZones();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
        super.setXAttr(path, name, value, flag);
    }

    @Override
    public byte[] getXAttr(Path path, String name) throws IOException {
        return super.getXAttr(path, name);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path) throws IOException {
        return super.getXAttrs(path);
    }

    @Override
    public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
        return super.getXAttrs(path, names);
    }

    @Override
    public List<String> listXAttrs(Path path) throws IOException {
        return super.listXAttrs(path);
    }

    @Override
    public void removeXAttr(Path path, String name) throws IOException {
        super.removeXAttr(path, name);
    }

    @Override
    public void access(Path path, FsAction mode) throws IOException {
        super.access(path, mode);
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException {
        return super.addDelegationTokens(renewer, credentials);
    }

    @Override
    public DFSInotifyEventInputStream getInotifyEventStream() throws IOException {
        return super.getInotifyEventStream();
    }

    @Override
    public DFSInotifyEventInputStream getInotifyEventStream(long lastReadTxid) throws IOException {
        return super.getInotifyEventStream(lastReadTxid);
    }

    @Override
    public Path getTrashRoot(Path path) {
        return super.getTrashRoot(path);
    }

    @Override
    public Collection<FileStatus> getTrashRoots(boolean allUsers) {
        return super.getTrashRoots(allUsers);
    }

    @Override
    @Deprecated
    public String getName() {
        return super.getName();
    }

    @Override
    public Path makeQualified(Path path) {
        return super.makeQualified(path);
    }

    @Override
    @InterfaceAudience.LimitedPrivate({"hdfs"})
    @VisibleForTesting
    public FileSystem[] getChildFileSystems() {
        return super.getChildFileSystems();
    }

    @Override
    public FsServerDefaults getServerDefaults(Path p) throws IOException {
        return super.getServerDefaults(p);
    }

    @Override
    public Path resolvePath(Path p) throws IOException {
        return super.resolvePath(p);
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        return super.open(f);
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        return super.create(f);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
        return super.create(f, overwrite);
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
        return super.create(f, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication) throws IOException {
        return super.create(f, replication);
    }

    @Override
    public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException {
        return super.create(f, replication, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException {
        return super.create(f, overwrite, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException {
        return super.create(f, overwrite, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException {
        return super.create(f, overwrite, bufferSize, replication, blockSize);
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return super.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return super.create(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return super.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return super.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean createNewFile(Path f) throws IOException {
        return super.createNewFile(f);
    }

    @Override
    public FSDataOutputStream append(Path f) throws IOException {
        return super.append(f);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
        return super.append(f, bufferSize);
    }

    @Override
    @Deprecated
    public short getReplication(Path src) throws IOException {
        return super.getReplication(src);
    }

    @Override
    @Deprecated
    public boolean delete(Path f) throws IOException {
        return super.delete(f);
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
        return super.deleteOnExit(f);
    }

    @Override
    public boolean cancelDeleteOnExit(Path f) {
        return super.cancelDeleteOnExit(f);
    }

    @Override
    public boolean exists(Path f) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_hdfs_OPERATION(?, ?)")) {
                statement.setString(1, f.toUri().getPath());
                statement.setString(2, "exists");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for exists");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    if (bytes.length != 1) {
                        throw new IOException("Wrong response size for exists");
                    }
                    return bytes[0] == 1;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean isDirectory(Path f) throws IOException {
        return super.isDirectory(f);
    }

    @Override
    public boolean isFile(Path f) throws IOException {
        return super.isFile(f);
    }

    @Override
    @Deprecated
    public long getLength(Path f) throws IOException {
        return super.getLength(f);
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException {
        return super.listStatus(f, filter);
    }

    @Override
    public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException {
        return super.listStatus(files);
    }

    @Override
    public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException {
        return super.listStatus(files, filter);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        return super.globStatus(pathPattern);
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException {
        return super.globStatus(pathPattern, filter);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException {
        return super.listLocatedStatus(f);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException {
        return super.listFiles(f, recursive);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return super.mkdirs(f);
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst) throws IOException {
        super.copyFromLocalFile(src, dst);
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
        super.moveFromLocalFile(srcs, dst);
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
        super.moveFromLocalFile(src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        super.copyFromLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
        super.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
        super.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    @Override
    public void copyToLocalFile(Path src, Path dst) throws IOException {
        super.copyToLocalFile(src, dst);
    }

    @Override
    public void moveToLocalFile(Path src, Path dst) throws IOException {
        super.moveToLocalFile(src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
        super.copyToLocalFile(delSrc, src, dst);
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException {
        super.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        return super.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
        super.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

    @Override
    public long getUsed() throws IOException {
        return super.getUsed();
    }

    @Override
    @Deprecated
    public long getBlockSize(Path f) throws IOException {
        return super.getBlockSize(f);
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return super.getDefaultBlockSize(f);
    }

    @Override
    public short getDefaultReplication(Path path) {
        return super.getDefaultReplication(path);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum) {
        super.setWriteChecksum(writeChecksum);
    }

    @Override
    public FsStatus getStatus() throws IOException {
        return delegate.getStatus();
    }

    @Override
    public void setXAttr(Path path, String name, byte[] value) throws IOException {
        super.setXAttr(path, name, value);
    }

    @Override
    public Configuration getConf() {
        return delegate.getConf();
    }
}
