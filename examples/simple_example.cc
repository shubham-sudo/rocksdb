#include <string>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <thread>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/table.h"

using namespace rocksdb;
std::string kDBPath = "/tmp/cs561_project1";

inline void showProgress(const uint64_t& workload_size, const uint64_t& counter) {

    if (counter / (workload_size / 100) >= 1) {
        for (int i = 0; i < 104; i++) {
            std::cout << "\b";
            fflush(stdout);
        }
    }
    for (int i = 0; i < counter / (workload_size / 100); i++) {
        std::cout << "=";
        fflush(stdout);
    }
    std::cout << std::setfill(' ') << std::setw(101 - counter / (workload_size / 100));
    std::cout << counter * 100 / workload_size << "%";
    fflush(stdout);

    if (counter == workload_size) {
        std::cout << "\n";
        return;
    }
}


inline void sleep_for_ms(uint32_t ms) {
   std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// Need to select timeout carefully
// Completion not guaranteed
bool CompactionMayAllComplete(DB *db) {
    uint64_t pending_compact;
    uint64_t pending_compact_bytes;
    uint64_t running_compact;
    bool success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
                            && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
                            && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
    std::cout << "Compaction Running : " << success << std::endl;
    std::cout << "Pending Compaction : " << pending_compact << std::endl;
    std::cout << "Pending Compact Bytes : " << pending_compact_bytes << std::endl;
    std::cout << "Running compaction : " << running_compact << std::endl;
    while ((pending_compact &&  pending_compact_bytes != 0) || running_compact || !success) {
        sleep_for_ms(200);
        success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
                            && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
                            && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
        // sleep_for_ms(60000);
        // std::cout << "#############" << std::endl; 
        // std::cout << "Pending Compaction : " << pending_compact << std::endl;
        // std::cout << "Pending Compact Bytes : " << pending_compact_bytes << std::endl;
        // std::cout << "Running compaction : " << running_compact << std::endl;
    }
    sleep_for_ms(30000);
    return true;
}

void runWorkload(Options& op, WriteOptions& write_op, ReadOptions& read_op) {
    DB* db;

    op.create_if_missing = true;
    op.write_buffer_size = 512;
    op.max_bytes_for_level_base = 512;
    op.max_bytes_for_level_multiplier = 2;
    op.target_file_size_base = 256;
    op.target_file_size_multiplier = 1;

    {
        op.memtable_factory = std::shared_ptr<VectorRepFactory>(new VectorRepFactory);
        op.allow_concurrent_memtable_write = false;
    }

    {
        //op.memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory);
    }

    {
        //op.memtable_factory = std::shared_ptr<MemTableRepFactory>(NewHashSkipListRepFactory());
	//op.allow_concurrent_memtable_write = false;
    }

    {
        //op.memtable_factory = std::shared_ptr<MemTableRepFactory>(NewHashLinkListRepFactory());
	//op.allow_concurrent_memtable_write = false;
    }

    //BlockBasedTableOptions table_options;
    //table_options.block_cache = NewLRUCache(8*1048576);
    //op.table_factory.reset(NewBlockBasedTableFactory(table_options));

    Status s = DB::Open(op, kDBPath, &db);
    if (!s.ok()) std::cerr << s.ToString() << std::endl;
    assert(s.ok());

    // opening workload file for the first time
    std::ifstream workload_file;
    workload_file.open("workload.txt");
    assert(workload_file);
    // doing a first pass to get the workload size
    uint64_t workload_size = 0;
    std::string line;
    while (std::getline(workload_file, line))
        ++workload_size;
    workload_file.close();

    workload_file.open("workload.txt");
    assert(workload_file);

    // Clearing the system cache
    std::cout << "Clearing system cache ..." << std::endl;
    int clean_flag = system("echo qwerty123@ | sudo -S sh -c 'echo 3 >/proc/sys/vm/drop_caches'");
    if (clean_flag) {
        std::cerr << "Cannot clean the system cache" << std::endl;
        exit(0);
    }

    Iterator* it = db->NewIterator(read_op); // for range reads
    uint64_t counter = 0; // for progress bar

    // time variables for measuring the time taken by the workload
    std::chrono::time_point<std::chrono::system_clock> start, end;
    std::chrono::time_point<std::chrono::system_clock> insert_start, insert_end;
    std::chrono::time_point<std::chrono::system_clock> query_start, query_end;
    std::chrono::time_point<std::chrono::system_clock> rquery_start, rquery_end;
    std::chrono::duration<double> total_insert_time_elapsed {0};
    std::chrono::duration<double> total_query_time_elapsed {0};
    std::chrono::duration<double> total_rquery_time_elapsed {0};
    start = std::chrono::system_clock::now();

    while (!workload_file.eof()) {
        char instruction;
        long key, start_key, end_key;
        std::string value;
        workload_file >> instruction;
        Slice _start_key{};
        Slice _end_key{};

        if (instruction == 'S' || instruction == 'I') {
            //######################## RocksDB STATS ###########################

            std::string property;
            std::string live_sst_property;
            bool result = db->GetProperty("rocksdb.levelstats", &property);
            bool live_sst_file_size = db->GetProperty("rocksdb.live-sst-files-size", &live_sst_property);

            if (result){
                std::cout << property << std::endl;
            }
            if (live_sst_file_size) {
                std::cout << live_sst_property << std::endl;
            }

            //##################################################################
        }

        switch (instruction)
        {
        case 'I': // insert
            // start measuring the time taken by the insert
            insert_start = std::chrono::system_clock::now();
            workload_file >> key >> value;
            // Put key-value
            s = db->Put(write_op, std::to_string(key), value);
            if (!s.ok()) std::cerr << s.ToString() << std::endl;
            assert(s.ok());
            counter++;
            // end measuring the time taken by the insert
            insert_end = std::chrono::system_clock::now();
            total_insert_time_elapsed += insert_end - insert_start;
            break;

        case 'Q': // probe: point query
            // start measuring the time taken by the query
            query_start = std::chrono::system_clock::now();
            workload_file >> key;
            s = db->Get(read_op, std::to_string(key), &value);
            //if (!s.ok()) std::cerr << s.ToString() << "key = " << key << std::endl;
            // assert(s.ok());
            counter++;
            // end measuring the time taken by the query
            query_end = std::chrono::system_clock::now();
            total_query_time_elapsed += query_end - query_start;
            break;

        case 'S': // scan: range query
            rquery_start = std::chrono::system_clock::now();
            workload_file >> start_key >> end_key;
            it->Refresh();
            assert(it->status().ok());
            for (it->Seek(std::to_string(start_key)); it->Valid(); it->Next()) {
                // std::cout << "found key = " << it->key().ToString() << std::endl;
                if (it->key().ToString() == std::to_string(end_key)) {
                    break;
                }
            }
            _start_key = std::to_string(start_key);
            _end_key = std::to_string(end_key);
            db->RangeQueryDrivenCompaction(nullptr, _start_key, _end_key);

            if (!it->status().ok()) {
                std::cerr << it->status().ToString() << std::endl;
            }
            counter++;
            rquery_end = std::chrono::system_clock::now();
            total_rquery_time_elapsed += rquery_end - rquery_start;
            break;

        default:
            std::cerr << "ERROR: Case match NOT found !!" << std::endl;
            break;
        }

        if (workload_size < 100) workload_size = 100;
        if (counter % (workload_size / 100) == 0) {
            showProgress(workload_size, counter);
        }
    }

    // end measuring the time taken by the workload
    // and printing the results
    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "\n----------------------Workload Complete-----------------------" << std::endl;
    std::cout << "Total time taken by workload = " << elapsed_seconds.count() << " seconds" << std::endl;
    std::cout << "Total time taken by inserts = " << total_insert_time_elapsed.count() << " seconds" << std::endl;
    std::cout << "Total time taken by queries = " << total_query_time_elapsed.count() << " seconds" << std::endl;
    std::cout << "Total time taken by rqueries = " << total_rquery_time_elapsed.count() << " seconds" << std::endl;

    workload_file.close();
    CompactionMayAllComplete(db);
    s = db->Close();
    if (!s.ok()) std::cerr << s.ToString() << std::endl;
    assert(s.ok());
    delete db;

    std::cout << "\n----------------------Closing DB-----------------------" << std::endl;

    return;
}

int main() {
    Options options;
    WriteOptions write_op;
    ReadOptions read_op;
    runWorkload(options, write_op, read_op);
}
