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
#include "rocksdb/statistics.h"


using namespace rocksdb;
std::string kDBPath = "/tmp/cs561_project1";

void configCompactionOptions(Options& op);
void printStats(DB* db, Options& options);

inline void sleep_for_ms(uint32_t ms) {
   std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

void writeVectorToCsv(const std::vector<std::chrono::duration<double>>& vec, const std::string& filename) {
    // open the file for writing
    std::ofstream outputFile(filename);

    // write the vector to the file in CSV format
    for (int i = 0; i < vec.size(); i++) {
        outputFile << vec[i].count();
        //std::cout << "Vector: " << vec[i].count() << std::endl;
        if (i != vec.size() - 1) {
            outputFile << ",";
        }
    }
    outputFile << std::endl;

    // close the file
    outputFile.close();
}

// Need to select timeout carefully Completion not guaranteed
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
    }

    sleep_for_ms(30000);
    return true;
}

void runWorkload(Options& op, WriteOptions& write_op, ReadOptions& read_op, int argc, char* argv[]) {
    DB* db;
    std::vector<std::chrono::duration<double>> rangeQTime;
    std::string rangeQTimeFileName = argv[1];

    Status s = DB::Open(op, kDBPath, &db);
    if (!s.ok()) std::cerr << s.ToString() << std::endl;
    assert(s.ok());

    // opening workload file for the first time
    std::ifstream workload_file;

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

    // printStats(db, op);

    if(argc > 2 && std::strcmp(argv[2], "--rc-off") == 0){
        std::cout << "RDC is off" << std::endl;
    }

    while (!workload_file.eof()) {
        char instruction;
        long key, start_key, end_key;
        std::string value;
        workload_file >> instruction;
        Slice _start_key{};
        Slice _end_key{};
        bool success = false;

        switch (instruction)
        {
        case 'U':
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
            _start_key = Slice(std::to_string(start_key));
            _end_key = Slice(std::to_string(end_key));
            if (argc > 2 && std::strcmp(argv[2], "--rc-off") == 0) {
                //std::cout << "RDC is off" << std::endl;
            } else {
                db->RangeQueryDrivenCompaction(_start_key, _end_key);
            }
            
            counter++;
            rquery_end = std::chrono::system_clock::now();
            total_rquery_time_elapsed += rquery_end - rquery_start;
            rangeQTime.push_back((rquery_end - rquery_start));
            //std::cout << "Range Query Time : " << (rquery_end - rquery_start).count() << std::endl;
            break;

        case 'D':  // Delete key
            workload_file >> key;
            s = db->Delete(write_op, std::to_string(key));
            counter++;
            break;


        default:
            std::cerr << "ERROR: Case match NOT found !!" << std::endl;
            break;
        }

    }

    // end measuring the time taken by the workload and printing the results
    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
    std::cout << "\n----------------------Workload Complete-----------------------" << std::endl;
    std::cout << "Total time taken by workload = " << elapsed_seconds.count() << " seconds" << std::endl;
    std::cout << "Total time taken by inserts and updates = " << total_insert_time_elapsed.count() << " seconds" << std::endl;
    std::cout << "Total time taken by queries = " << total_query_time_elapsed.count() << " seconds" << std::endl;
    std::cout << "Total time taken by rqueries = " << total_rquery_time_elapsed.count() << " seconds" << std::endl;
    // for( auto i : rangeQTime){
    //     std::cout << i << std::endl;
    // }
    writeVectorToCsv(rangeQTime, rangeQTimeFileName);

    workload_file.close();
    printStats(db, op);
    CompactionMayAllComplete(db);
    printStats(db, op);

    s = db->Close();
    if (!s.ok()) std::cerr << s.ToString() << std::endl;
    assert(s.ok());
    delete db;

    std::cout << "\n----------------------Closing DB-----------------------" << std::endl;

    return;
}

// Helper function to filter specific lines from a multiline string
// std::string filter_stats(const std::string &input, const std::vector<std::string> &keywords) {
//   std::stringstream ss(input);
//   std::string line;
//   std::stringstream filtered;
//   while (std::getline(ss, line)) {
//     for (const auto &keyword : keywords) {
//       if (line.find(keyword) != std::string::npos) {
//         filtered << line << std::endl;
//         break;
//       }
//     }
//   }
//   return filtered.str();
// }

void printStats(DB* db, Options& options) {
    std::string each_level_stats;
    std::string sst_file_size;

    bool result = db->GetProperty("rocksdb.levelstats", &each_level_stats);
    bool live_sst_file_size = db->GetProperty("rocksdb.live-sst-files-size", &sst_file_size);

    std::cout << std::endl;
    std::cout << "Level Statistics" << std::endl;

    if (result){
        std::cout << "Level, Total Files, " << each_level_stats << std::endl;  // printing level stats
    }
    if (live_sst_file_size) {
        std::cout << "Total SST Files Size : " << sst_file_size << std::endl;  // printing sst file size
    }
    std::cout << "----------------------------------------" << std::endl;

    std::string all_stats = options.statistics->ToString();

    // Filter specific statistics
    // std::vector<std::string> keywords = {
    //     "rocksdb.compaction.times.micros",
    //     "rocksdb.db.iter.bytes.read",
    //     "rocksdb.no.file.opens",
    //     "rocksdb.db.seek.micros"
    // };

    // std::string filtered_stats = filter_stats(all_stats, keywords);

    std::cout << std::endl;
    std::cout << "RocksDB Statistics : " << std::endl;
    // std::cout << filtered_stats << std::endl;
    std::cout << all_stats << std::endl;
    std::cout << "----------------------------------------" << std::endl;
}

void configCompactionOptions(Options& op) {
    op.create_if_missing = true;

    // toggle auto compaction
    op.compaction_style = kCompactionStyleLevel;
    op.disable_auto_compactions = false;

    // Memory allocation options
    op.write_buffer_size = 512 * 1024;
    op.max_write_buffer_number = 1;  // max number of memtables in memory
    op.memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory);  // hard coding to skiplist

    // compaction options
    op.level_compaction_dynamic_level_bytes = false;
    op.compaction_filter = nullptr;
    op.compaction_filter_factory = nullptr;
    op.access_hint_on_compaction_start = DBOptions::AccessHint::NONE;
    op.level0_file_num_compaction_trigger = 1;

    op.target_file_size_base = 512 * 1024;  // file size in level base, usually level-1)
    op.target_file_size_multiplier = 1;
    op.max_background_jobs = 4;
    // op.max_compaction_bytes = op.target_file_size_base * 25;  // Set to default
    op.max_bytes_for_level_base = op.write_buffer_size;  // same as write buffer size
    op.max_bytes_for_level_multiplier = 10;
    // op.merge_operator;
    // op.soft_pending_compaction_bytes_limit;
    // op.hard_pending_compaction_bytes_limit;
    // op.use_direct_io_for_flush_and_compaction;
    op.num_levels = 7;  // kept default

    // statistics
    op.statistics = CreateDBStatistics();
}

int main(int argc, char* argv[]) {
    Options options;
    WriteOptions write_op;
    ReadOptions read_op;
    configCompactionOptions(options);
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <outputRangeStatFileName>" << std::endl;
        return 1;
    }
    runWorkload(options, write_op, read_op, argc, argv);
}
