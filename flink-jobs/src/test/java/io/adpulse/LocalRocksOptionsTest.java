package io.adpulse;

import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class LocalRocksOptionsTest {
    @TempDir Path directory;

    @Test void pointLookupsAndIterationSurviveFlushAndReopen() throws Exception {
        RocksDB.loadLibrary();
        var factory = new LocalRocksOptions();
        for (int pass = 0; pass < 2; pass++) {
            var nativeHandles = new ArrayList<AutoCloseable>();
            try (var dbOptions = factory.createDBOptions(new DBOptions().setCreateIfMissing(true), nativeHandles);
                 var columns = factory.createColumnOptions(new ColumnFamilyOptions(), nativeHandles)) {
                var handles = new ArrayList<ColumnFamilyHandle>();
                try (var db = RocksDB.open(dbOptions, directory.toString(),
                        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columns)), handles)) {
                    try {
                        if (pass == 0) {
                            for (int i = 0; i < 2000; i++) db.put(bytes("group/contribution-" + i), bytes("value-" + i));
                            try (var flush = new FlushOptions().setWaitForFlush(true)) { db.flush(flush); }
                        }
                        for (int i = 0; i < 2000; i++) {
                            assertArrayEquals(bytes("value-" + i), db.get(bytes("group/contribution-" + i)));
                            assertNull(db.get(bytes("group/absent-" + i)));
                        }
                        int count = 0;
                        try (var iterator = db.newIterator()) {
                            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) count++;
                            iterator.status();
                        }
                        assertEquals(2000, count);
                    } finally { for (var handle : handles) handle.close(); }
                }
            } finally { for (var handle : nativeHandles) handle.close(); }
        }
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
}
