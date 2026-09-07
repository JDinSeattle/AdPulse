package io.adpulse;

import java.util.Collection;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;

/** Many small column families share a bounded per-slot managed-memory budget.
 * 64 MiB default memtables allocate large arenas and caused repeated flush stalls
 * in the measured 2 GiB / four-slot local deployment. Keep managed memory enabled.
 */
public final class LocalRocksOptions implements RocksDBOptionsFactory {
    private static final long serialVersionUID = 1L;
    @Override public DBOptions createDBOptions(DBOptions options, Collection<AutoCloseable> handles) {
        return options.setMaxBackgroundJobs(4);
    }
    @Override public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions options, Collection<AutoCloseable> handles) {
        // Contribution dedup probes are predominantly misses as new business IDs arrive.
        // Keep whole-key filtering: a key-group prefix filter cannot reject a new map key.
        var table = options.tableFormatConfig() instanceof BlockBasedTableConfig existing
            ? existing : new BlockBasedTableConfig();
        var filter = new BloomFilter(10, false);
        handles.add(filter); // Flink closes native resources with the backend lifecycle.
        table.setFilterPolicy(filter).setWholeKeyFiltering(true);
        options.setTableFormatConfig(table);
        return options.setWriteBufferSize(8L * 1024 * 1024).setArenaBlockSize(256L * 1024)
            .setMaxWriteBufferNumber(4).setMinWriteBufferNumberToMerge(1);
    }
}
