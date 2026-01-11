#include "sf_tsd_index.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <format> // C++20 formatting library
#include <iostream>
#include <random>
#include <thread>
#include <vector>

constexpr uint32_t kNumOfSensors = 1024;
static uint64_t g_epoch_start[kNumOfSensors] = {0};
static std::atomic<uint32_t> g_insert_sensor_id{0};
static std::atomic<uint32_t> g_get_sensor_id{0};
static std::atomic<uint32_t> g_agg_sensor_id{0};
static std::atomic<uint32_t> g_scan_sensor_id{0};
static std::atomic<uint32_t> g_mix_sensor_id{0};
static std::atomic<uint32_t> g_snapshot_sensor_id{0};
static std::vector<FullTsdTuple> sf_tsd_data[kNumOfSensors];
static uint64_t sf_tsd_append_ts[kNumOfSensors];

/*
 * Rdtsc() - This function returns the value of the time stamp counter
 *           on the current core
 */
inline uint64_t Rdtsc() {
#if (__x86__ || __x86_64__)
  uint32_t lo, hi;
  asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return (((uint64_t)hi << 32) | lo);
#else
  uint64_t val;
  asm volatile("mrs %0, cntvct_el0" : "=r"(val));
  return val;
#endif
}

// Test utility: Timer (dependency-free)
class Timer {
public:
  void Start() { start_ = std::chrono::high_resolution_clock::now(); }
  double EndMs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start_)
        .count();
  }
  double EndUs() const {
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start_)
        .count();
  }

private:
  std::chrono::high_resolution_clock::time_point start_;
};

#if 1
void GenerateTimeSeriesData(size_t num_records) {
  if (num_records == 0)
    return;

  // uint32_t sensor_id = 0;
  const uint64_t start_ts = 1756684800000ULL; // 2025/09/01 00:00:00.0
  // 1756684800000
  // g_epoch_start = start_ts - (start_ts % kMsPerDay);
  const uint64_t per_sensor_capacity =
      (num_records + (kNumOfSensors - 1)) / kNumOfSensors;
  std::cout << "per sendor capacity: " << per_sensor_capacity << std::endl;
  for (uint64_t i = 0; i < kNumOfSensors; ++i) {
    auto &per_sensor_data = sf_tsd_data[i];
    per_sensor_data.reserve(per_sensor_capacity);
    for (uint64_t j = 0; j < per_sensor_capacity; ++j) {
      const uint64_t key = start_ts + j * 50; // 50ms frequency
      per_sensor_data.push_back(
          {key, key}); // same value for simplify verification
    }
    // for mix workload append insert
    sf_tsd_append_ts[i] = start_ts + per_sensor_capacity * 50;
  }
}
#else
void GenerateTimeSeriesData(size_t num_records) {
  if (num_records == 0)
    return;

  const uint64_t per_sensor_capacity =
      (num_records + (kNumOfSensors - 1)) / kNumOfSensors;
  std::cout << "per sendor capacity: " << per_sensor_capacity << std::endl;
  for (uint64_t i = 0; i < kNumOfSensors; ++i) {
    auto &per_sensor_data = sf_tsd_data[i];
    per_sensor_data.reserve(per_sensor_capacity);
    for (uint64_t j = 0; j < per_sensor_capacity; ++j) {
      uint64_t key = 0;
      if (i == 0) {
        // Generate Rdtsc timstamp
        key = Rdtsc();
      } else {
        // Align to 1st sensor for snapshot query
        key = sf_tsd_data[i - 1][j].key;
      }
      per_sensor_data.push_back({key, key});
    }

    g_epoch_start[i] = per_sensor_data[0].key & ~(kMsPerTimeSpan - 1);
    // for mix workload append insert
    sf_tsd_append_ts[i] = per_sensor_data[per_sensor_capacity - 1].key + 1;
  }
}
#endif

// Test utility: Generate random query keys (randomly selected from dataset)
std::vector<Timestamp>
GenerateRandomQueryKeys(const std::vector<FullTsdTuple> &data,
                        size_t num_queries) {
  std::vector<Timestamp> keys(num_queries);
  if (data.empty() || num_queries == 0)
    return keys;

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dist(0, data.size() - 1);

  for (size_t i = 0; i < num_queries; ++i) {
    keys[i] = data[dist(gen)].key;
  }
  return keys;
}

// Thread function: Execute Insert operation
void InsertThreadFunc(FlatTsdIndex global_index[], double &elapsed_time) {
  Timer timer;
  timer.Start();

  while (true) {
    const uint32_t sensor_id =
        g_insert_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumOfSensors) {
      break;
    }

    const auto &tuples = sf_tsd_data[sensor_id];
    const size_t count = tuples.size();
    global_index[sensor_id].Reset();
    for (size_t m = 0; m < count; ++m) {
      global_index[sensor_id].Insert(tuples[m].key, tuples[m].value);
    }
  }

  elapsed_time = timer.EndUs();
}

// Thread function: Execute Get operation
void GetThreadFunc(FlatTsdIndex global_index[], double &elapsed_time,
                   size_t &found_count) {
  Value value;
  Timer timer;
  timer.Start();
  found_count = 0;

  while (true) {
    const uint32_t sensor_id =
        g_get_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumOfSensors) {
      break;
    }

    const auto &tuples = sf_tsd_data[sensor_id];
    const size_t count = tuples.size();
    global_index[sensor_id].Reset();
    for (size_t m = 0; m < count; ++m) {
      if (global_index[sensor_id].Get(tuples[m].key, value)) {
        found_count++;
      }
    }
  }

  elapsed_time = timer.EndUs();
}

// Thread function: Execute Agg operation
void AggThreadFunc(FlatTsdIndex global_index[], double &elapsed_time,
                   size_t &found_count) {
  ExternalStats stats;
  Timer timer;
  timer.Start();
  found_count = 0;

  while (true) {
    const uint32_t sensor_id =
        g_agg_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumOfSensors) {
      break;
    }

    const auto &tuples = sf_tsd_data[sensor_id];
    const size_t count = tuples.size();
    global_index[sensor_id].Reset();
    global_index[sensor_id].Stats(tuples[0].key, tuples[count - 1].key, stats);
    found_count += count;
  }

  elapsed_time = timer.EndUs();
}

// Thread function: Execute 100% scan (consistent with Blink-hash: one scan per record)
void ScanThreadFunc(FlatTsdIndex global_index[], double &elapsed_time,
                    size_t &scan_requests, size_t &scan_rows) {
  std::vector<std::vector<FullTsdTuple>> scan_results;

  // Thread-safe fast random number generator, avoids multi-thread degradation caused by rand() global lock
  thread_local uint64_t fast_rand_state = std::random_device{}();
  auto fast_rand = [&](uint32_t mod) -> uint32_t {
    fast_rand_state ^= fast_rand_state << 13;
    fast_rand_state ^= fast_rand_state >> 7;
    fast_rand_state ^= fast_rand_state << 17;
    return fast_rand_state % mod;
  };

  Timer timer;
  timer.Start();
  scan_requests = 0;
  scan_rows = 0;

  while (true) {
    const uint32_t sensor_id =
        g_scan_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumOfSensors) {
      break;
    }

    const auto &tuples = sf_tsd_data[sensor_id];
    const size_t count = tuples.size();

    for (size_t m = 0; m < count; ++m) {
      // Consistent with Blink-hash: scan each record once, range rand()%100
      uint64_t range = fast_rand(100);
      const Timestamp start_ts = tuples[m].key;
      const Timestamp end_ts =
          start_ts + range; // Use range directly, consistent with Blink-hash
      scan_results = global_index[sensor_id].Scan(start_ts, end_ts);
      scan_requests++; // Count number of requests
      for (const auto &elem : scan_results) {
        scan_rows += elem.size(); // Auxiliary metric: returned row count
      }
    }
  }

  elapsed_time = timer.EndUs();
}

// Thread function: Execute Mix operation 50% Insert/30% Short Scan/10% Long Scan/10% Get
void MixThreadFunc(FlatTsdIndex global_index[], double &elapsed_time,
                   size_t &found_count, size_t &scan_requests,
                   size_t &scan_rows, size_t &insert_count) {
  Value value;
  std::vector<std::vector<FullTsdTuple>> scan_results;

  // Pre-generate random number sequence - eliminate runtime generation overhead
  thread_local std::vector<int> random_sequence;
  thread_local size_t random_pos = 0;
  if (random_sequence.empty()) {
    random_sequence.reserve(200000); // Pre-generate 200k random numbers
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 99);
    for (size_t i = 0; i < 200000; ++i) {
      random_sequence.push_back(dist(gen));
    }
  }

  // Fast random number generator - replacement for rand()
  thread_local uint64_t fast_rand_state = std::random_device{}();
  auto fast_rand = [&](uint32_t mod) -> uint32_t {
    fast_rand_state ^= fast_rand_state << 13;
    fast_rand_state ^= fast_rand_state >> 7;
    fast_rand_state ^= fast_rand_state << 17;
    return fast_rand_state % mod;
  };

  Timer timer;
  timer.Start();
  found_count = 0;
  scan_requests = 0;
  scan_rows = 0;
  insert_count = 0;

  while (true) {
    const uint32_t sensor_id =
        g_mix_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumOfSensors) {
      break;
    }

    auto &append_ts = sf_tsd_append_ts[sensor_id];
    const auto &tuples = sf_tsd_data[sensor_id];
    const size_t count = tuples.size();
    global_index[sensor_id].Reset();

    for (size_t m = 0; m < count; ++m) {
      const int r = random_sequence[random_pos++ %
                                    random_sequence.size()]; // Pre-generated random number
      if (r < 50) {
        // Insert new time-series point
        append_ts += 50;
        global_index[sensor_id].Insert(append_ts, append_ts);
        insert_count++;
      } else if (r < 80) {
        // Short Range scan (30%)
        uint64_t range = 50 * (fast_rand(5) + 5); // 5 - 10 tuples
        const Timestamp start_ts = tuples[m].key;
        const Timestamp end_ts = start_ts + range;
        scan_results = global_index[sensor_id].Scan(start_ts, end_ts);
        scan_requests++; // Count number of requests
        for (const auto &elem : scan_results) {
          scan_rows += elem.size(); // Auxiliary metric: returned row count
        }
      } else if (r < 90) {
        // Long Range scan (10%)
        uint64_t range = 50 * (fast_rand(90) + 10); // 10 - 100 tuples
        const Timestamp start_ts = tuples[m].key;
        const Timestamp end_ts = start_ts + range;
        scan_results = global_index[sensor_id].Scan(start_ts, end_ts);
        scan_requests++; // Count number of requests
        for (const auto &elem : scan_results) {
          scan_rows += elem.size(); // Auxiliary metric: returned row count
        }
      } else {
        // Point read (10%)
        if (global_index[sensor_id].Get(tuples[m].key, value)) {
          found_count++;
        }
      }
    }
  }

  elapsed_time = timer.EndUs();
}

// Thread function: Execute Snapshot operation
void SnapsotThreadFunc(FlatTsdIndex global_index[], double &elapsed_time,
                       size_t &found_count) {
  Value snapshot_results[kNumOfSensors];
  Timer timer;
  timer.Start();
  found_count = 0;

  while (true) {
    const uint32_t sensor_id =
        g_snapshot_sensor_id.fetch_add(1, std::memory_order_relaxed);
    if (sensor_id >= kNumOfSensors) {
      break;
    }

    const auto &tuples = sf_tsd_data[sensor_id];
    uint64_t snapshot_ts = tuples[0].key + sensor_id * 50;
    // Ensure it is a valid timestamp for snapshot query
    snapshot_ts = std::min(snapshot_ts, tuples[tuples.size() - 1].key);
    for (size_t m = 0; m < kNumOfSensors; ++m) {
      if (global_index[m].Get(snapshot_ts, snapshot_results[m])) {
        found_count++;
      }
    }
  }

  elapsed_time = timer.EndUs();
  // std::cout << "Snapshot cost: " << elapsed_time << " us" << std::endl;
}

// -------------------------- Main function (execute all tests) --------------------------
int main(int argc, char *argv[]) {
  uint32_t num_of_threads = 1;
  if (argc == 2) {
    num_of_threads = std::atoi(argv[1]);
    // Validate thread count is reasonable
    if (num_of_threads == 0 || num_of_threads > 64) {
      std::cout << "Invalid thread count, using 1 thread instead." << std::endl;
      num_of_threads = 1;
    }
  }
  std::cout << "Running with " << num_of_threads << " threads..." << std::endl;

  FlatTsdIndex *global_index = new FlatTsdIndex[kNumOfSensors];
  const uint64_t kTestSize = 100000000; // 100 million records
  std::cout << "Generating test data (" << kTestSize << " records)..."
            << std::endl;
  GenerateTimeSeriesData(kTestSize);
  // return 0;

  // for (size_t n = 0; n < kNumOfSensors; ++n) {
  //   global_index[n].SetEpochStart(g_epoch_start[n]);
  // }

  for (size_t n = 0; n < kNumOfSensors; ++n) {
    global_index[n].SetTagId(n);
  }
  std::cout << "Data generation complete." << std::endl;

  // -------------------------- Test Insert performance --------------------------
  {
    std::cout << "\nTesting Insert performance..." << std::endl;
    std::vector<std::thread> insert_threads;
    std::vector<double> insert_times(num_of_threads, 0.0);
    insert_threads.reserve(num_of_threads);

    Timer total_insert_timer;
    total_insert_timer.Start();

    // Create insert threads
    for (uint32_t t = 0; t < num_of_threads; ++t) {
      insert_threads.emplace_back(InsertThreadFunc, global_index,
                                  std::ref(insert_times[t]));
    }

    // Wait for all insert threads to complete
    for (auto &thread : insert_threads) {
      thread.join();
    }

    double total_insert_time = total_insert_timer.EndUs();

    // Calculate insert performance statistics
    double min_insert =
        *std::min_element(insert_times.begin(), insert_times.end());
    double max_insert =
        *std::max_element(insert_times.begin(), insert_times.end());
    double avg_insert = 0.0;
    for (double t : insert_times)
      avg_insert += t;
    avg_insert /= num_of_threads;

    std::cout << "Insert complete. "
              << "Total time: " << total_insert_time / 1000 << "ms"
              << " (" << kTestSize / (total_insert_time / 1000000) / 1000000
              << " Mops/sec)" << std::endl;
    std::cout << "Thread insert times (us): Min=" << min_insert
              << ", Max=" << max_insert << ", Avg=" << avg_insert << std::endl;
  }

  //
  for (size_t n = 0; n < kNumOfSensors; ++n) {
    global_index[n].Compaction();
  }

  // -------------------------- Warm up segment tree build --------------------------
  {
    std::cout << "\nWarming up segment tree (triggering build)..." << std::endl;
    Timer build_timer;
    build_timer.Start();

    for (size_t n = 0; n < kNumOfSensors; ++n) {
      const auto &tuples = sf_tsd_data[n];
      if (tuples.empty())
        continue;
      const Timestamp start_ts = tuples[0].key;
      const Timestamp end_ts = tuples[tuples.size() - 1].key;
      ExternalStats stats;
      global_index[n].Stats(start_ts, end_ts, stats);
    }

    double build_time = build_timer.EndUs();
    std::cout << "Build trigger complete. Time: " << build_time / 1000 << "ms"
              << std::endl;

    // Wait for async build to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    std::cout << "Segment tree ready." << std::endl;
  }

  // -------------------------- Test Get performance --------------------------
  {
    std::cout << "\nTesting Get performance..." << std::endl;
    std::vector<std::thread> get_threads;
    std::vector<double> get_times(num_of_threads, 0.0);
    std::vector<size_t> found_counts(num_of_threads, 0);
    get_threads.reserve(num_of_threads);

    Timer total_get_timer;
    total_get_timer.Start();

    // Create query threads
    for (uint32_t t = 0; t < num_of_threads; ++t) {
      get_threads.emplace_back(GetThreadFunc, global_index,
                               std::ref(get_times[t]),
                               std::ref(found_counts[t]));
    }

    // Wait for all query threads to complete
    for (auto &thread : get_threads) {
      thread.join();
    }

    double total_get_time = total_get_timer.EndUs();

    // Calculate query performance statistics
    size_t total_found = 0;
    for (size_t c : found_counts)
      total_found += c;

    double min_get = *std::min_element(get_times.begin(), get_times.end());
    double max_get = *std::max_element(get_times.begin(), get_times.end());
    double avg_get = 0.0;
    for (double t : get_times)
      avg_get += t;
    avg_get /= num_of_threads;

    std::cout << "Get complete. "
              << "Total time: " << total_get_time / 1000 << "ms"
              << " (" << kTestSize / (total_get_time / 1000000) / 1000000
              << " Mops/sec)" << std::endl;
    std::cout << "Found " << total_found << " of " << kTestSize << " ("
              << (total_found * 100.0 / kTestSize) << "%)" << std::endl;
    std::cout << "Thread get times (us): Min=" << min_get << ", Max=" << max_get
              << ", Avg=" << avg_get << std::endl;
  }

  // -------------------------- Test Agg performance --------------------------
  {
    std::cout << "\nTesting Agg performance..." << std::endl;

    // Round 1: May include build cost
    {
      std::cout << "Round 1 (may include build cost)..." << std::endl;
      g_agg_sensor_id.store(0, std::memory_order_relaxed);
      std::vector<std::thread> get_threads;
      std::vector<double> get_times(num_of_threads, 0.0);
      std::vector<size_t> found_counts(num_of_threads, 0);
      get_threads.reserve(num_of_threads);

      Timer total_get_timer;
      total_get_timer.Start();

      for (uint32_t t = 0; t < num_of_threads; ++t) {
        get_threads.emplace_back(AggThreadFunc, global_index,
                                 std::ref(get_times[t]),
                                 std::ref(found_counts[t]));
      }

      for (auto &thread : get_threads) {
        thread.join();
      }

      double total_get_time = total_get_timer.EndUs();
      size_t total_found = 0;
      for (size_t c : found_counts)
        total_found += c;

      double min_get = *std::min_element(get_times.begin(), get_times.end());
      double max_get = *std::max_element(get_times.begin(), get_times.end());
      double avg_get = 0.0;
      for (double t : get_times)
        avg_get += t;
      avg_get /= num_of_threads;

      std::cout << "Round 1 complete. "
                << "Total time: " << total_get_time / 1000 << "ms"
                << " (" << total_found / (total_get_time / 1000000) / 1000000
                << " Mops/sec)" << std::endl;
      std::cout << "Thread agg times (us): Min=" << min_get
                << ", Max=" << max_get << ", Avg=" << avg_get << std::endl;
    }

    // Round 2: Pure query
    {
      std::cout << "Round 2 (pure query)..." << std::endl;
      g_agg_sensor_id.store(0, std::memory_order_relaxed);
      std::vector<std::thread> get_threads;
      std::vector<double> get_times(num_of_threads, 0.0);
      std::vector<size_t> found_counts(num_of_threads, 0);
      get_threads.reserve(num_of_threads);

      Timer total_get_timer;
      total_get_timer.Start();

      for (uint32_t t = 0; t < num_of_threads; ++t) {
        get_threads.emplace_back(AggThreadFunc, global_index,
                                 std::ref(get_times[t]),
                                 std::ref(found_counts[t]));
      }

      for (auto &thread : get_threads) {
        thread.join();
      }

      double total_get_time = total_get_timer.EndUs();
      size_t total_found = 0;
      for (size_t c : found_counts)
        total_found += c;

      double min_get = *std::min_element(get_times.begin(), get_times.end());
      double max_get = *std::max_element(get_times.begin(), get_times.end());
      double avg_get = 0.0;
      for (double t : get_times)
        avg_get += t;
      avg_get /= num_of_threads;

      std::cout << "Round 2 complete. "
                << "Total time: " << total_get_time / 1000 << "ms"
                << " (" << total_found / (total_get_time / 1000000) / 1000000
                << " Mops/sec)" << std::endl;
      std::cout << "Found " << total_found << " of " << kTestSize << " ("
                << (total_found * 100.0 / kTestSize) << "%)" << std::endl;
      std::cout << "Thread agg times (us): Min=" << min_get
                << ", Max=" << max_get << ", Avg=" << avg_get << std::endl;
    }
  }

  // -------------------------- Test Scan performance --------------------------
  {
    std::cout << "\nTesting Scan performance..." << std::endl;
    std::vector<std::thread> get_threads;
    std::vector<double> get_times(num_of_threads, 0.0);
    std::vector<size_t> scan_requests_vec(num_of_threads, 0);
    std::vector<size_t> scan_rows_vec(num_of_threads, 0);
    get_threads.reserve(num_of_threads);

    Timer total_get_timer;
    total_get_timer.Start();

    // Create query threads
    for (uint32_t t = 0; t < num_of_threads; ++t) {
      get_threads.emplace_back(
          ScanThreadFunc, global_index, std::ref(get_times[t]),
          std::ref(scan_requests_vec[t]), std::ref(scan_rows_vec[t]));
    }

    // Wait for all query threads to complete
    for (auto &thread : get_threads) {
      thread.join();
    }

    double total_get_time = total_get_timer.EndUs();

    // 计算查询性能统计
    size_t total_scan_requests = 0;
    for (size_t c : scan_requests_vec)
      total_scan_requests += c;

    size_t total_scan_rows = 0;
    for (size_t c : scan_rows_vec)
      total_scan_rows += c;

    double min_get = *std::min_element(get_times.begin(), get_times.end());
    double max_get = *std::max_element(get_times.begin(), get_times.end());
    double avg_get = 0.0;
    for (double t : get_times)
      avg_get += t;
    avg_get /= num_of_threads;

    // Primary metric: request throughput
    double tput_mops = total_scan_requests / (total_get_time / 1e6) / 1e6;
    std::cout << "Scan requests: " << total_scan_requests
              << ", throughput: " << tput_mops << " Mops/sec" << std::endl;
    // Auxiliary metric
    std::cout << "Avg rows/scan: "
              << (double)total_scan_rows / total_scan_requests << std::endl;
    std::cout << "Thread scan times (us): Min=" << min_get
              << ", Max=" << max_get << ", Avg=" << avg_get << std::endl;
  }

  // -------------------------- Test Mix performance --------------------------
  {
    std::cout << "\nTesting Mix performance..." << std::endl;
    std::vector<std::thread> get_threads;
    std::vector<double> get_times(num_of_threads, 0.0);
    std::vector<size_t> found_counts(num_of_threads, 0);
    std::vector<size_t> scan_requests_vec(num_of_threads, 0);
    std::vector<size_t> scan_rows_vec(num_of_threads, 0);
    std::vector<size_t> insert_counts(num_of_threads, 0);
    get_threads.reserve(num_of_threads);

    Timer total_get_timer;
    total_get_timer.Start();

    // Create query threads
    for (uint32_t t = 0; t < num_of_threads; ++t) {
      get_threads.emplace_back(
          MixThreadFunc, global_index, std::ref(get_times[t]),
          std::ref(found_counts[t]), std::ref(scan_requests_vec[t]),
          std::ref(scan_rows_vec[t]), std::ref(insert_counts[t]));
    }

    // Wait for all query threads to complete
    for (auto &thread : get_threads) {
      thread.join();
    }

    double total_get_time = total_get_timer.EndUs();

    // Calculate query performance statistics
    size_t total_found = 0;
    for (size_t c : found_counts)
      total_found += c;

    size_t total_scan_requests = 0;
    for (size_t c : scan_requests_vec)
      total_scan_requests += c;

    size_t total_scan_rows = 0;
    for (size_t c : scan_rows_vec)
      total_scan_rows += c;

    size_t total_insert = 0;
    for (size_t c : insert_counts)
      total_insert += c;

    double min_get = *std::min_element(get_times.begin(), get_times.end());
    double max_get = *std::max_element(get_times.begin(), get_times.end());
    double avg_get = 0.0;
    for (double t : get_times)
      avg_get += t;
    avg_get /= num_of_threads;

    // Primary metric: request throughput (kTestSize requests)
    double tput_mops = kTestSize / (total_get_time / 1e6) / 1e6;
    std::cout << "Mix requests: " << kTestSize << ", throughput: " << tput_mops
              << " Mops/sec" << std::endl;
    // Distribution statistics
    std::cout << "Insert: " << total_insert << " ("
              << (total_insert * 100.0 / kTestSize) << "%)" << std::endl;
    std::cout << "Scan requests: " << total_scan_requests << " ("
              << (total_scan_requests * 100.0 / kTestSize) << "%), "
              << "avg rows/scan: "
              << (total_scan_requests > 0
                      ? (double)total_scan_rows / total_scan_requests
                      : 0)
              << std::endl;
    std::cout << "Get: " << kTestSize * 10 / 100 << " requests, found "
              << total_found << " ("
              << (total_found * 100.0 / (kTestSize * 10 / 100)) << "% hit)"
              << std::endl;
    std::cout << "Thread mix times (us): Min=" << min_get << ", Max=" << max_get
              << ", Avg=" << avg_get << std::endl;
  }

  // -------------------------- Test Snapshot performance --------------------------
  {
    std::cout << "\nTesting Snapshot performance..." << std::endl;
    std::vector<std::thread> get_threads;
    std::vector<double> get_times(num_of_threads, 0.0);
    std::vector<size_t> found_counts(num_of_threads, 0);
    get_threads.reserve(num_of_threads);

    Timer total_get_timer;
    total_get_timer.Start();

    // Create query threads
    for (uint32_t t = 0; t < num_of_threads; ++t) {
      get_threads.emplace_back(SnapsotThreadFunc, global_index,
                               std::ref(get_times[t]),
                               std::ref(found_counts[t]));
    }

    // Wait for all query threads to complete
    for (auto &thread : get_threads) {
      thread.join();
    }

    double total_get_time = total_get_timer.EndUs();

    // Calculate query performance statistics
    size_t total_found = 0;
    for (size_t c : found_counts)
      total_found += c;

    double min_get = *std::min_element(get_times.begin(), get_times.end());
    double max_get = *std::max_element(get_times.begin(), get_times.end());
    double avg_get = 0.0;
    for (double t : get_times)
      avg_get += t;
    avg_get /= num_of_threads;

    std::cout << "Snapshot complete. "
              << "Total time: " << total_get_time / 1000 << "ms"
              << " (" << total_found / (total_get_time / 1000000) / 1000000
              << " Mops/sec)" << std::endl;
    std::cout << "Found " << total_found << " of " << kTestSize << " ("
              << (total_found * 100.0 / kTestSize) << "%)" << std::endl;
    std::cout << "Thread snapshot times (us): Min=" << min_get
              << ", Max=" << max_get << ", Avg=" << avg_get << std::endl;
  }

  delete[] global_index;
  return 0;
}
