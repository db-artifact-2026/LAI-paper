#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib> // std::aligned_alloc
#include <cstring>
#include <functional>
#include <functional>

#include <iostream>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <sys/time.h> // for gettimeofday
#include <thread>
#include <time.h> // for clock_gettime
#include <tuple>
#include <vector>

// Debug logging switch (can be disabled in production)
#define DEBUG_LOG 1
#if DEBUG_LOG
// Fix: support variable arguments, use printf for formatted output
#define LOG_DEBUG(fmt, ...)                                                    \
  do {                                                                         \
    std::printf("[DEBUG] %s: " fmt "\n", __func__, ##__VA_ARGS__);             \
  } while (0)
#else
#define LOG_DEBUG(msg, ...)
#endif

// -------------------------- Basic Types and Constants --------------------------
using Timestamp = uint64_t;
using Value = uint64_t;
constexpr size_t kCacheLineSize = 64;

// Time-series data structure
struct alignas(16) FullTsdTuple final {
  Timestamp key;
  Value value;
  FullTsdTuple(void) noexcept {}
  FullTsdTuple(Timestamp k, Value v) noexcept : key(k), value(v) {}
  constexpr bool operator<(const FullTsdTuple &rhs) const noexcept {
    return key < rhs.key;
  }
};

struct MiniTsdTuple final {
  uint32_t key;
  Value value;
  constexpr bool operator<(const MiniTsdTuple &rhs) const noexcept {
    return key < rhs.key;
  }
};

struct alignas(64) ExternalStats final {
  uint64_t min = std::numeric_limits<uint64_t>::max();
  uint64_t max = std::numeric_limits<uint64_t>::min();
  uint64_t sum = 0;
  uint64_t avg = 0;
};

struct alignas(64) InternalStats final {
  uint64_t min = std::numeric_limits<uint64_t>::max();
  uint64_t max = std::numeric_limits<uint64_t>::min();
  uint64_t sum = 0;
  uint64_t cnt = 0;
};

// Time constants (milliseconds)
static constexpr uint64_t kPerIntervalExponent = 18;   // 18
static constexpr uint64_t kPerTimeSpanExponent = 21;   // 21
static constexpr Timestamp kMsPerTimeSpan = (1 << 21); // 2^21
static constexpr Timestamp kMsPerTimeSpanMinus1 = (1 << 21) - 1;
static constexpr Timestamp kMsPerInterval = (1 << 18); // 2^16
static constexpr Timestamp kMsPerIntervalMinus1 = (1 << 18) - 1;
static constexpr size_t kIntervalsPerTimeSpan = 8;
static constexpr Timestamp kHigh4BytesMask = 0xFFFFFFFF00000000ULL;

// Cache configuration
static constexpr size_t kPrimaryBufferCapacity = 6000; // 5 mins: 5*60*20
static constexpr size_t kStandbyBufferCapacity = 6000;
static constexpr size_t kPivotSamplesSize = 32;

// Threshold Const
static constexpr size_t kDegradeThreshold = 16;
static constexpr size_t kTimeSpanCapacity = 256;

class WriteBuffer final {
public:
  explicit WriteBuffer(size_t capacity)
      : data_(capacity), capacity_(capacity) {}

  // Short spin helper function
  inline __attribute__((always_inline)) static void SpinPause() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(_M_ARM64)
    asm volatile("yield");
#else
    std::this_thread::yield();
#endif
  }

  inline __attribute__((always_inline)) bool Append(Timestamp key,
                                                    Value value) noexcept {
    // Step 1: Atomic index allocation (ensures each thread gets unique index)
    const size_t idx = alloc_idx_.fetch_add(1, std::memory_order_relaxed);
    if (idx >= capacity_)
      [[unlikely]] { return false; }

    // Step 2: Safe data write (each thread writes to different position)
    data_[idx] = {key, value};

    // Step 3: Wait for previous position to commit (ensures ordering), commit data in order
    size_t expected = idx;
    while (!commit_idx_.compare_exchange_weak(expected, idx + 1,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
      // CAS failed, previous position not committed yet, wait
      expected = idx; // Reset expected
      std::this_thread::yield();
    }

    return true;
  }

  // Batch append: reserve continuous range at once, all succeed or all fail (returns 0 on insufficient capacity)
  inline __attribute__((always_inline)) size_t
  BatchAppend(std::span<const FullTsdTuple> kvs) noexcept {
    const size_t n = kvs.size();
    if (n == 0)
      [[unlikely]] { return 0; }

    // Step 1: CAS预留连续区间 [start, start+n)
    size_t start_idx = 0;
    size_t cur = alloc_idx_.load(std::memory_order_relaxed);
    while (true) {
      if (cur + n > capacity_)
        [[unlikely]] { return 0; }
      if (alloc_idx_.compare_exchange_weak(cur, cur + n,
                                           std::memory_order_relaxed,
                                           std::memory_order_relaxed)) {
        start_idx = cur;
        break;
      }
      SpinPause();
    }

    // Step 2: Batch write data (ranges do not overlap, no additional synchronization needed)
    for (size_t i = 0; i < n; ++i) {
      data_[start_idx + i] = kvs[i];
    }

    // Step 3: Sequential commit (wait for previous completion)
    size_t expected = start_idx;
    size_t spin_count = 0;
    static constexpr size_t kShortSpin = 64;
    while (!commit_idx_.compare_exchange_weak(expected, start_idx + n,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
      expected = start_idx;
      if (++spin_count <= kShortSpin) {
        SpinPause();
      } else {
        std::this_thread::yield();
        spin_count = 0;
      }
    }
    return n;
  }

  struct SnapshotView final {
    const FullTsdTuple *data;
    size_t size;
  };

  SnapshotView Snapshot() const noexcept {
    const size_t safe_size =
        std::min(commit_idx_.load(std::memory_order_acquire), capacity_);
    return {data_.data(), safe_size};
  }

  bool IsFull() const noexcept {
    return alloc_idx_.load(std::memory_order_relaxed) >= capacity_;
  }

  size_t capacity() const noexcept { return capacity_; }
  size_t size() const noexcept {
    return std::min(commit_idx_.load(std::memory_order_acquire), capacity_);
  }

  inline __attribute__((always_inline)) bool Lookup(Timestamp key,
                                                    Value &value) const
      noexcept {
    const size_t safe_size = commit_idx_.load(std::memory_order_acquire);
    const size_t read_size = std::min(safe_size, capacity_);

    for (size_t i = 0; i < read_size; ++i) {
      if (data_[i].key == key) {
        value = data_[i].value;
        return true;
      }
    }

    return false;
  }

  inline __attribute__((always_inline)) std::vector<FullTsdTuple>
  Scan(Timestamp start, Timestamp end) const noexcept {
    const size_t safe_size = commit_idx_.load(std::memory_order_acquire);
    const size_t read_size = std::min(safe_size, capacity_);
    if (read_size == 0)
      [[unlikely]] { return {}; }

    std::vector<FullTsdTuple> results;
    results.reserve(read_size); // TODO: pre-allocate reasonable size
    for (size_t i = 0; i < read_size; ++i) {
      if (data_[i].key >= start && data_[i].key <= end) {
        results.emplace_back(data_[i]);
      }
    }

    std::sort(results.begin(), results.end());
    return results;
  }

  inline __attribute__((always_inline)) InternalStats Stats(Timestamp start,
                                                            Timestamp end) const
      noexcept {
    const size_t safe_size = commit_idx_.load(std::memory_order_acquire);
    const size_t read_size = std::min(safe_size, capacity_);
    if (read_size == 0)
      [[unlikely]] { return {}; }

    InternalStats stats;
    for (size_t i = 0; i < read_size; ++i) {
      if (data_[i].key >= start && data_[i].key <= end) {
        if (data_[i].value < stats.min) {
          stats.min = data_[i].value;
        }
        if (data_[i].value > stats.max) {
          stats.max = data_[i].value;
        }
        stats.sum += data_[i].value;
        stats.cnt += 1;
      }
    }

    return stats;
  }

private:
  const size_t capacity_;
  alignas(64) std::atomic<size_t> alloc_idx_{0};
  alignas(64) std::atomic<size_t> commit_idx_{0};
  std::vector<FullTsdTuple> data_;
};

class SortedBuffer final {
public:
  explicit SortedBuffer(const WriteBuffer::SnapshotView &view) {
    if (view.size == 0)
      [[unlikely]] return;

    // Copy lower 4 bytes and sort
    data_.reserve(view.size);
    for (size_t i = 0; i < view.size; ++i) {
      internal_stats_.sum += view.data[i].value;
      data_.emplace_back(static_cast<uint32_t>(view.data[i].key),
                         view.data[i].value);
    }
    std::sort(data_.begin(), data_.end());

    // Update stats
    internal_stats_.min = data_.front().value;
    internal_stats_.max = data_.back().value;
    internal_stats_.cnt = data_.size();

    // Construct pivot samples to accelerate queries (lookup and scan)
    if (data_.size() > (kPivotSamplesSize << 1))
      [[likely]] {
        const double sample_window =
            static_cast<double>(data_.size() - 1) / (kPivotSamplesSize - 1);
        for (size_t i = 0; i < kPivotSamplesSize; i++) {
          const size_t idx =
              std::min(static_cast<size_t>(std::llround(i * sample_window)),
                       data_.size() - 1);
          pivot_samples_[i] = {data_[idx].key, static_cast<uint32_t>(idx)};
        }
      }
    else
      [[unlikely]] {
        // Too little data, skip build pivot samples
        pivot_samples_[0].first = std::numeric_limits<uint32_t>::min();
      }
  }

  inline __attribute__((always_inline)) bool Lookup(Timestamp key,
                                                    Value &value) const
      noexcept {
    if (data_.empty())
      [[unlikely]] { return false; }
    const uint32_t key_low = static_cast<uint32_t>(key);

    // Step-1: Interpolation search probing
    const size_t size = data_.size();
    const uint32_t min_key = data_.front().key;
    const uint32_t max_key = data_.back().key;
    if (key_low < min_key || key_low > max_key) {
      return false;
    }

    // Calculate interpolation position
    size_t pos = 0;
    // Avoid division by zero (all key values are the same)
    if (max_key != min_key)
      [[unlikely]] {
        // Use integer arithmetic to avoid floating point overhead
        const uint64_t key_range = static_cast<uint64_t>(max_key) - min_key;
        const uint64_t target_offset = static_cast<uint64_t>(key_low) - min_key;

        // Calculate interpolation position
        pos = static_cast<size_t>((target_offset * (size - 1)) / key_range);
      }

    // Ensure position is within valid range
    pos = std::min(pos, size - 1);
    // Determine probe range [start, end]
    size_t start = (pos >= 2) ? pos - 2 : 0;
    size_t end = (pos + 2 < size) ? pos + 2 : size - 1;

    // Linear probe within range
    for (size_t i = start; i <= end; ++i) {
      if (data_[i].key == key_low)
        [[likely]] {
          value = data_[i].value;
          // std::cout << "hit !!!" << std::endl;
          return true; // Found target
        }
    }

    // Step-2: Search in pivot samples first
    auto left_it = data_.begin();
    auto right_it = data_.end();
    if (pivot_samples_[0].first != std::numeric_limits<uint32_t>::min())
      [[likely]] {
        bool found = false;
        for (size_t n = 1; n < kPivotSamplesSize; ++n) {
          if (key_low >= pivot_samples_[n - 1].first &&
              key_low < pivot_samples_[n].first) {
            left_it = data_.begin() + pivot_samples_[n - 1].second;
            right_it = data_.begin() + pivot_samples_[n].second;

            // Exactly match
            if (key_low == pivot_samples_[n - 1].first) {
              value = data_[pivot_samples_[n - 1].second].value;
              return true;
            }

            if (key_low == pivot_samples_[n].first) {
              value = data_[pivot_samples_[n].second].value;
              return true;
            }

            found = true;
            break;
          }
        }

        if (!found) {
          // Because pivot_samples_ covers the entire data range, if no
          // sub-interval is found in pivot_samples_, it means that the target
          // does not exist.
          return false;
        }
      }

    // Step-3: Normal search
    auto it = std::lower_bound(
        left_it, right_it, key_low,
        [](const MiniTsdTuple &kv, uint32_t k) noexcept { return kv.key < k; });

    if (it != right_it && it->key == key_low)
      [[likely]] {
        value = it->value;
        return true;
      }

    return false;
  }

  inline __attribute__((always_inline)) std::vector<FullTsdTuple>
  Scan(Timestamp start, Timestamp end) const noexcept {
    if (data_.empty())
      [[unlikely]] { return {}; }

    const uint32_t start_low = static_cast<uint32_t>(start);
    const uint32_t end_low = static_cast<uint32_t>(end);
    const size_t data_size = data_.size();
    const uint64_t high_bytes = start & kHigh4BytesMask;
    std::vector<FullTsdTuple> results;

    // Step-0: Fast-fail or Fast-path check
    if (end_low < data_.front().key || start_low > data_.back().key)
      [[unlikely]] { return {}; }

    if (start_low <= data_.front().key && end_low >= data_.back().key)
      [[likely]] {
        results.reserve(data_size);
        for (size_t i = 0; i < data_size; ++i) {
          results.push_back({high_bytes | data_[i].key, data_[i].value});
        }
        return results;
      }

    // Step-1: Interpolation search
    // TODO: since scan is fast enough

    // Step-2: Search in pivot samples first
    auto left_it = data_.begin();
    auto right_it = data_.end();
    if (pivot_samples_[0].first != std::numeric_limits<uint32_t>::min())
      [[likely]] {
        bool left_found = false;
        for (size_t n = 1; n < kPivotSamplesSize; ++n) {
          if (!left_found && start_low >= pivot_samples_[n - 1].first &&
              start_low < pivot_samples_[n].first) {
            left_it = data_.begin() + pivot_samples_[n - 1].second;
            left_found = true;
          }

          if (end_low >= pivot_samples_[n - 1].first &&
              end_low < pivot_samples_[n].first) {
            right_it = data_.begin() + pivot_samples_[n].second;
            break;
          }
        }
      }

    // Step-3: Normal Search
    auto left = std::lower_bound(
        left_it, right_it, start_low,
        [](const MiniTsdTuple &kv, uint32_t k) noexcept { return kv.key < k; });
    auto right = std::upper_bound(
        left_it, right_it, end_low,
        [](uint32_t k, const MiniTsdTuple &kv) noexcept { return k < kv.key; });
    if (left >= right) {
      // assert(0);
      return {};
    }

    const size_t results_size = std::distance(left, right);
    // std::cout << "results_size: " << results_size << std::endl;
    results.reserve(results_size);
    for (auto iter = left; iter != right; ++iter) {
      results.emplace_back(high_bytes | iter->key, iter->value);
    }

    return results;
  }

  inline __attribute__((always_inline)) InternalStats Stats(Timestamp start,
                                                            Timestamp end) const
      noexcept {
    if (data_.empty())
      [[unlikely]] { return {}; }

    const uint32_t start_low = static_cast<uint32_t>(start);
    const uint32_t end_low = static_cast<uint32_t>(end);
    const size_t data_size = data_.size();

    // Step-0: Fast-fail or Fast-path check
    // Fast-fail
    if (end_low < data_.front().key || start_low > data_.back().key)
      [[unlikely]] { return {}; }

    // Fast-path
    if (start_low <= data_.front().key && end_low >= data_.back().key) {
      return internal_stats_;
    }

    // Step-1: Interpolation search
    // TODO: since scan is fast enough

    // Step-2: Search in pivot samples first
    auto left_it = data_.begin();
    auto right_it = data_.end();
    if (pivot_samples_[0].first != std::numeric_limits<uint32_t>::min())
      [[likely]] {
        bool left_found = false;
        for (size_t n = 1; n < kPivotSamplesSize; ++n) {
          if (!left_found && start_low >= pivot_samples_[n - 1].first &&
              start_low < pivot_samples_[n].first) {
            left_it = data_.begin() + pivot_samples_[n - 1].second;
            left_found = true;
          }

          if (end_low >= pivot_samples_[n - 1].first &&
              end_low < pivot_samples_[n].first) {
            right_it = data_.begin() + pivot_samples_[n].second;
            break;
          }
        }
      }

    // Step-3: Normal Search
    auto left = std::lower_bound(
        left_it, right_it, start_low,
        [](const MiniTsdTuple &kv, uint32_t k) noexcept { return kv.key < k; });
    auto right = std::upper_bound(
        left_it, right_it, end_low,
        [](uint32_t k, const MiniTsdTuple &kv) noexcept { return k < kv.key; });
    if (left >= right) {
      // assert(0);
      return {};
    }

    InternalStats stats;
    for (auto iter = left; iter != right; ++iter) {
      if (iter->value < stats.min) {
        stats.min = iter->value;
      }
      if (iter->value > stats.max) {
        stats.max = iter->value;
      }
      stats.sum += iter->value;
      stats.cnt += 1;
    }

    return stats;
  }

  const std::vector<MiniTsdTuple> &data() const noexcept { return data_; }
  bool empty() const noexcept { return data_.empty(); }
  size_t size() const noexcept { return data_.size(); }

private:
  std::vector<MiniTsdTuple> data_;
  alignas(64) InternalStats internal_stats_;
  alignas(64) std::pair<uint32_t, uint32_t> pivot_samples_[kPivotSamplesSize];
};

class IntervalStore final {
public:
  enum class State : uint32_t {
    kActiveOpen = 0,
    kClosedUnsorted = 1,
    kClosedSorted = 2
  };

  explicit IntervalStore() noexcept
      : state_(State::kActiveOpen),
        primary_buffer_(new WriteBuffer(kPrimaryBufferCapacity)),
        standby_buffer_(new WriteBuffer(kStandbyBufferCapacity)) {}

  IntervalStore(const IntervalStore &) = delete;
  IntervalStore &operator=(const IntervalStore &) = delete;

  ~IntervalStore() noexcept {
    if (nullptr != sorted_buffer_) {
      delete sorted_buffer_;
    }

    if (nullptr != standby_buffer_) {
      delete standby_buffer_;
    }

    if (nullptr != primary_buffer_) {
      delete primary_buffer_;
    }
  }

  inline __attribute__((always_inline)) bool
  Insert(const Timestamp &key, const Value &value) noexcept {
    uint32_t attempt_count = 0;
    constexpr uint32_t kMaxAttempt = 5;

    while (attempt_count++ < kMaxAttempt) {
      const State current_state = state_.load(std::memory_order_acquire);

      switch (current_state) {
      case State::kActiveOpen: {
        if (primary_buffer_->Append(key, value))
          [[likely]] { return true; }

        // Fast check with relaxed memory order
        if (state_.load(std::memory_order_relaxed) != State::kActiveOpen) {
          continue;
        }

        // Double check: state may have changed
        if (state_.load(std::memory_order_acquire) != State::kActiveOpen) {
          continue;
        }

        // CAS ensures only one thread executes state transition
        State expected = State::kActiveOpen;
        if (state_.compare_exchange_strong(expected, State::kClosedUnsorted,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
          // State transition successful, continue trying to write to standby
        }
        break;
      }

      case State::kClosedUnsorted:
      case State::kClosedSorted: {
        if (standby_buffer_->Append(key, value))
          [[likely]] { return true; }
        else {
          std::cout << "standby buffer full!" << std::endl;
          assert(0);
        }
        break;
      }

      default:
        __builtin_unreachable();
        return false;
      }
    }
    return false;
  }

  // Batch insert: returns actual number of successfully written entries
  inline __attribute__((always_inline)) size_t
  BatchInsert(std::span<const FullTsdTuple> kvs) noexcept {
    if (kvs.empty())
      [[unlikely]] { return 0; }

    size_t written = 0;
    constexpr uint32_t kMaxAttempt = 5;

    while (written < kvs.size()) {
      uint32_t attempts = 0;
      while (attempts++ < kMaxAttempt) {
        const State current_state = state_.load(std::memory_order_acquire);
        std::span<const FullTsdTuple> tail = kvs.subspan(written);

        switch (current_state) {
        case State::kActiveOpen: {
          const size_t w = primary_buffer_->BatchAppend(tail);
          if (w > 0)
            [[likely]] {
              written += w;
              goto next_round;
            }

          // Primary has no capacity: try to switch state, write to standby in next round
          State expected = State::kActiveOpen;
          state_.compare_exchange_strong(expected, State::kClosedUnsorted,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed);
          break;
        }

        case State::kClosedUnsorted:
        case State::kClosedSorted: {
          const size_t w = standby_buffer_->BatchAppend(tail);
          if (w > 0)
            [[likely]] {
              written += w;
              goto next_round;
            }
          // Standby also insufficient: return number of successfully written entries
          return written;
        }

        default:
          __builtin_unreachable();
          return written;
        }
      }
      // Multiple attempts failed, avoid infinite loop
      return written;

    next_round:;
    }

    return written;
  }

  inline __attribute__((always_inline)) bool Get(Timestamp key,
                                                 Value &value) const noexcept {
    const State current_state = state_.load(std::memory_order_acquire);
    switch (current_state) {
    case State::kActiveOpen: {
      if (primary_buffer_->Lookup(key, value)) {
        return true;
      }
      break;
    }

    case State::kClosedUnsorted: {
      if (primary_buffer_->Lookup(key, value)) {
        return true;
      }

      if (standby_buffer_->Lookup(key, value)) {
        return true;
      }
      break;
    }

    case State::kClosedSorted: {
      if (sorted_buffer_->Lookup(key, value)) {
        return true;
      }

      if (standby_buffer_->Lookup(key, value)) {
        return true;
      }
      break;
    }

    default:
      __builtin_unreachable();
      return false;
    }

    return false;
  }

  inline std::vector<FullTsdTuple> Scan(Timestamp start_key,
                                        Timestamp end_key) const noexcept {
    const State current_state = state_.load(std::memory_order_acquire);
    switch (current_state) {
    case State::kActiveOpen: {
      return primary_buffer_->Scan(start_key, end_key);
    }

    case State::kClosedUnsorted: {
      auto primary_results = primary_buffer_->Scan(start_key, end_key);
      auto standby_results = standby_buffer_->Scan(start_key, end_key);
      if (standby_results.empty()) {
        return primary_results;
      }

      // Remove duplicates when merging (if any)
      primary_results.reserve(primary_results.size() + standby_results.size());
      primary_results.insert(primary_results.end(),
                             std::make_move_iterator(standby_results.begin()),
                             std::make_move_iterator(standby_results.end()));
      std::sort(primary_results.begin(), primary_results.end());
      return primary_results;
    }

    case State::kClosedSorted: {
      auto sorted_results = sorted_buffer_->Scan(start_key, end_key);
      auto standby_results = standby_buffer_->Scan(start_key, end_key);
      if (standby_results.empty()) {
        return sorted_results;
      }

      // Remove duplicates when merging (if any)
      sorted_results.reserve(sorted_results.size() + standby_results.size());
      sorted_results.insert(sorted_results.end(),
                            std::make_move_iterator(standby_results.begin()),
                            std::make_move_iterator(standby_results.end()));
      std::sort(sorted_results.begin(), sorted_results.end());
      return sorted_results;
    }

    default:
      __builtin_unreachable();
      return {};
    }
  }

  inline InternalStats Stats(Timestamp start_key, Timestamp end_key) const
      noexcept {
    const State current_state = state_.load(std::memory_order_acquire);
    switch (current_state) {
    case State::kActiveOpen: {
      return primary_buffer_->Stats(start_key, end_key);
    }

    case State::kClosedUnsorted: {
      auto primary_results = primary_buffer_->Stats(start_key, end_key);
      const auto snapshot = standby_buffer_->Snapshot();
      if (snapshot.size == 0) {
        return primary_results;
      } else {
        auto standby_results = standby_buffer_->Stats(start_key, end_key);
        if (standby_results.min < primary_results.min) {
          primary_results.min = standby_results.min;
        }
        if (standby_results.max > primary_results.max) {
          primary_results.max = standby_results.max;
        }
        primary_results.sum += standby_results.sum;
        primary_results.cnt += standby_results.cnt;
        return primary_results;
      }
    }

    case State::kClosedSorted: {
      auto sorted_results = sorted_buffer_->Stats(start_key, end_key);
      const auto snapshot = standby_buffer_->Snapshot();
      if (snapshot.size == 0) {
        return sorted_results;
      } else {
        auto standby_results = standby_buffer_->Stats(start_key, end_key);
        if (standby_results.min < sorted_results.min) {
          sorted_results.min = standby_results.min;
        }
        if (standby_results.max > sorted_results.max) {
          sorted_results.max = standby_results.max;
        }
        sorted_results.sum += standby_results.sum;
        sorted_results.cnt += standby_results.cnt;
        return sorted_results;
      }
    }

    default:
      __builtin_unreachable();
      return {};
    }
  }

  inline void Close(void) noexcept {
    // TODO: EBR to ensure no cuncurrent Insert
    const WriteBuffer::SnapshotView snapshot = primary_buffer_->Snapshot();
    if (snapshot.size > 0) {
      sorted_buffer_ = new SortedBuffer(snapshot);
      state_.store(State::kClosedSorted, std::memory_order_release);
    }
  }

  // Get current state
  inline __attribute__((always_inline)) State GetState() const noexcept {
    return state_.load(std::memory_order_acquire);
  }

private:
  alignas(64) std::atomic<State> state_;
  alignas(64) WriteBuffer *primary_buffer_ = nullptr;
  alignas(64) WriteBuffer *standby_buffer_ = nullptr;
  alignas(64) SortedBuffer *sorted_buffer_ = nullptr;
};

class TimeSpanContainer final {
public:
  explicit TimeSpanContainer(uint64_t span_start) noexcept
      : span_start_(span_start),
        interval_stores_(new IntervalStore[kIntervalsPerTimeSpan]) {}
  TimeSpanContainer(const TimeSpanContainer &) = delete;
  TimeSpanContainer &operator=(const TimeSpanContainer &) = delete;
  ~TimeSpanContainer() noexcept { delete[] interval_stores_; }

  // Single insert
  inline __attribute__((always_inline)) bool
  Insert(uint16_t interval_id, const Timestamp &key,
         const Value &value) noexcept {
    return interval_stores_[interval_id].Insert(key, value);
  }

  // Batch insert: returns actual number of successfully written entries
  inline __attribute__((always_inline)) size_t
  BatchInsert(uint16_t interval_id, std::span<const FullTsdTuple> kvs) noexcept {
    return interval_stores_[interval_id].BatchInsert(kvs);
  }

  inline __attribute__((always_inline)) bool
  Get(Timestamp key, uint16_t interval_id, Value &value) const noexcept {
    return interval_stores_[interval_id].Get(key, value);
  }

  // Range scan
  inline std::vector<FullTsdTuple> Scan(uint16_t interval_id,
                                        Timestamp start_key,
                                        Timestamp end_key) const noexcept {
    return interval_stores_[interval_id].Scan(start_key, end_key);
  }

  inline InternalStats Stats(uint16_t interval_id, Timestamp start_key,
                             Timestamp end_key) const noexcept {
    return interval_stores_[interval_id].Stats(start_key, end_key);
  }
  inline __attribute__((always_inline)) IntervalStore::State
  GetIntervalState(uint16_t interval_id) const noexcept {
    return interval_stores_[interval_id].GetState();
  }

  inline __attribute__((always_inline)) uint64_t
  GetIntervalStart(size_t interval_id) const {
    return span_start_ + interval_start_offsets_[interval_id];
  }

  inline __attribute__((always_inline)) uint64_t
  GetIntervalEnd(size_t interval_id) const {
    return span_start_ + interval_end_offsets_[interval_id];
  }

  //
  void CloseAllIntervals(void) {
    for (size_t n = 0; n < kIntervalsPerTimeSpan; ++n) {
      interval_stores_[n].Close();
    }
  }

private:
  // Compile-time pre-computation: start offset of each interval relative to day 0:00 (milliseconds)
  static constexpr std::array<uint32_t, kIntervalsPerTimeSpan>
      interval_start_offsets_ = []() {
        std::array<uint32_t, kIntervalsPerTimeSpan> arr{};
        for (size_t i = 0; i < kIntervalsPerTimeSpan; ++i) {
          arr[i] = i * kMsPerInterval; // Multiplication completed at compile time
        }
        return arr;
      }();

  // Compile-time pre-computation: end offset of each interval relative to day 0:00 (milliseconds)
  static constexpr std::array<uint32_t, kIntervalsPerTimeSpan>
      interval_end_offsets_ = []() {
        std::array<uint32_t, kIntervalsPerTimeSpan> arr{};
        for (size_t i = 0; i < kIntervalsPerTimeSpan; ++i) {
          arr[i] = interval_start_offsets_[i] +
                   kMsPerIntervalMinus1; // Reuse pre-computed constant
        }
        return arr;
      }();

  const uint64_t span_start_;
  IntervalStore *interval_stores_;
};

// TODO: Adaptive and recycle
template <size_t Capacity> class TimeSpanContainerMgr final {
  static_assert(std::has_single_bit(Capacity), "Capacity must be power of 2");

private:
  struct alignas(kCacheLineSize) Bucket final {
    std::atomic<TimeSpanContainer *> container{nullptr};
    char padding[kCacheLineSize - sizeof(std::atomic<TimeSpanContainer *>)];
  };

  static constexpr size_t kCapacityMask = Capacity - 1;
  alignas(64) Bucket buckets_[Capacity];

public:
  TimeSpanContainerMgr() = default;
  TimeSpanContainerMgr(const TimeSpanContainerMgr &) = delete;
  TimeSpanContainerMgr &operator=(const TimeSpanContainerMgr &) = delete;

  // Debug
  // std::atomic<uint32_t> total_span_count_{0};

  TimeSpanContainer *Get(uint64_t timespan_start) {
    const size_t index =
        (timespan_start >> kPerTimeSpanExponent) & kCapacityMask;
    Bucket &bucket = buckets_[index];

    TimeSpanContainer *container =
        bucket.container.load(std::memory_order_acquire);
    if (nullptr != container)
      [[likely]] { return container; }

    // Debug
    // total_span_count_.fetch_add(1, std::memory_order_relaxed);
    TimeSpanContainer *tmp = new TimeSpanContainer(timespan_start);
    if (bucket.container.compare_exchange_strong(container, tmp,
                                                 std::memory_order_acq_rel,
                                                 std::memory_order_acquire))
      [[likely]] { return tmp; }
    else
      [[unlikely]] {
        delete tmp;
        return container;
      }
  }

  void Compaction(void) noexcept {
    uint32_t total_span_size = 0;
    for (size_t i = 0; i < Capacity; ++i) {
      Bucket &bucket = buckets_[i];
      TimeSpanContainer *container =
          bucket.container.load(std::memory_order_acquire);
      if (nullptr != container)
        [[likely]] {
          ++total_span_size;
          container->CloseAllIntervals();
        }
    }

    // std::cout << "total span size: " << total_span_size << std::endl;
  }

  // Access manager capacity
  inline __attribute__((always_inline)) size_t CapacityValue() const noexcept {
    return Capacity;
  }

  // Read container pointer by index
  inline __attribute__((always_inline)) TimeSpanContainer *GetAt(
      size_t idx) const noexcept {
    const Bucket &bucket = buckets_[idx];
    return bucket.container.load(std::memory_order_acquire);
  }
};

// --------- Global time-series index (partition management) ---------
class alignas(64) FlatTsdIndex final {
public:
  FlatTsdIndex() : container_mgr_() {}
  FlatTsdIndex(const FlatTsdIndex &) = delete;
  FlatTsdIndex &operator=(const FlatTsdIndex &) = delete;
  ~FlatTsdIndex() noexcept = default;

  // inline void SetEpochStart(uint64_t epoch_start) noexcept {
  //  epoch_start_ = epoch_start;
  //}

  inline __attribute__((always_inline)) void
  SetTagId(uint32_t tag_id) noexcept {
    tag_id_ = tag_id;
  }

  // Single insert
  inline __attribute__((always_inline)) bool
  Insert(const Timestamp &key, const Value &value) noexcept {
    uint64_t timespan_start;
    size_t interval_id;
    TimeSpanContainer *timespan_container;
    CalculateContext(key, timespan_start, interval_id, timespan_container);
    const bool ok = timespan_container->Insert(interval_id, key, value);
    if (ok) {
      segment_tree_ready_.store(false, std::memory_order_release);
    }
    return ok;
  }

  // Batch insert: returns actual number of successfully written entries
  // Suggestion: kvs in non-decreasing key order reduces grouping times (not required to be sorted, unordered results in more fragments but still correct)
  size_t BatchInsert(const std::vector<FullTsdTuple> &kvs) noexcept {
    const size_t n = kvs.size();
    if (n == 0)
      [[unlikely]] { return 0; }

    static constexpr size_t kMaxChunk = 64; // Limit single batch size to reduce CAS contention

    size_t total = 0;
    size_t start_pos = 0;

    // Context of first data entry
    uint64_t last_timespan_start;
    size_t last_interval_id;
    TimeSpanContainer *last_container;
    CalculateContext(kvs[0].key, last_timespan_start, last_interval_id,
                     last_container);

    for (size_t i = 1; i <= n; ++i) {
      bool flush_batch = (i == n); // Last entry must be flushed

      if (!flush_batch) {
        uint64_t curr_timespan_start;
        size_t curr_interval_id;
        TimeSpanContainer *curr_container;
        CalculateContext(kvs[i].key, curr_timespan_start, curr_interval_id,
                         curr_container);

        // Check if crossing interval boundary
        flush_batch = (curr_timespan_start != last_timespan_start ||
                       curr_interval_id != last_interval_id);
      }

      // Force split for overly large batches
      if (!flush_batch) {
        flush_batch = ((i - start_pos) >= kMaxChunk);
      }

      if (flush_batch) {
        const size_t batch_size = i - start_pos;
        std::span<const FullTsdTuple> batch(&kvs[start_pos], batch_size);

        const size_t inserted = last_container->BatchInsert(
            static_cast<uint16_t>(last_interval_id), batch);
        total += inserted;

        // Standby also insufficient: degrade remaining elements to individual Insert, ensuring zero loss
        if (inserted < batch_size) {
          for (size_t j = inserted; j < batch_size; ++j) {
            const auto &kv = batch[j];
            if (last_container->Insert(static_cast<uint16_t>(last_interval_id),
                                        kv.key, kv.value)) {
              ++total;
            }
          }
        }

        if (i < n) {
          // Update to next interval
          CalculateContext(kvs[i].key, last_timespan_start, last_interval_id,
                           last_container);
          start_pos = i;
        }
      }
    }

    // Mark segment tree as invalid
    if (total > 0) {
      segment_tree_ready_.store(false, std::memory_order_release);
    }

    return total;
  }

  inline __attribute__((always_inline)) bool Get(Timestamp key,
                                                 Value &value) noexcept {
    uint64_t timespan_start;
    size_t interval_id;
    TimeSpanContainer *timespan_container;
    CalculateContext(key, timespan_start, interval_id, timespan_container);
    return timespan_container->Get(key, static_cast<uint16_t>(interval_id),
                                   value);
  }

  // Range scan (cross-day, cross-interval)
  std::vector<std::vector<FullTsdTuple>> Scan(Timestamp start_key,
                                              Timestamp end_key) {
    if (start_key > end_key)
      [[unlikely]] throw std::invalid_argument("start > end");

    // 1. Calculate all (timespan + interval) to be scanned
    std::vector<std::pair<uint64_t, uint16_t>> di_list;
    di_list.reserve(
        (end_key - start_key + kMsPerIntervalMinus1) / kMsPerInterval + 1);
    uint64_t curr_timespan;
    size_t curr_interval;
    TimeSpanContainer *curr_timespan_container;
    CalculateContext(start_key, curr_timespan, curr_interval,
                     curr_timespan_container);
    di_list.emplace_back(curr_timespan, static_cast<uint16_t>(curr_interval));

    while (true) {
      // Next interval
      uint64_t next_timespan = curr_timespan;
      uint16_t next_interval = static_cast<uint16_t>(curr_interval + 1);
      if (next_interval >= kIntervalsPerTimeSpan) {
        next_timespan += kMsPerTimeSpan;
        next_interval = 0;
      }

      // Check if exceeding end time
      const Timestamp next_interval_start =
          next_timespan + static_cast<uint64_t>(next_interval) * kMsPerInterval;
      if (next_interval_start > end_key)
        break;

      di_list.emplace_back(next_timespan, next_interval);
      curr_timespan = next_timespan;
      curr_interval = next_interval;
    }

    // 2. Scan each (timespan + interval)
    std::vector<std::vector<FullTsdTuple>> results;
    results.reserve(di_list.size());
    for (const auto & [ timespan_start, interval_id ] : di_list) {
      const Timestamp interval_start =
          timespan_start + static_cast<uint64_t>(interval_id) * kMsPerInterval;
      const Timestamp interval_end = interval_start + kMsPerIntervalMinus1;
      // Calculate actual scan range for current interval
      const Timestamp scan_start = std::max(start_key, interval_start);
      const Timestamp scan_end = std::min(end_key, interval_end);
      // Execute scan
      CalculateContext(scan_start, curr_timespan, curr_interval,
                       curr_timespan_container);
      results.emplace_back(
          curr_timespan_container->Scan(interval_id, scan_start, scan_end));
    }

    return results;
  }

  // Range scan (cross-day, cross-interval)
  void Stats(Timestamp start_key, Timestamp end_key, ExternalStats &stats) {
    if (start_key > end_key)
      [[unlikely]] throw std::invalid_argument("start > end");
    const size_t interval_count =
        (end_key - start_key + kMsPerIntervalMinus1) / kMsPerInterval;

    if (segment_tree_ready_.load(std::memory_order_acquire) &&
        interval_count >= kIntervalsPerTimeSpan) {
      const InternalStats r = SegmentTreeQuery(segment_tree_root_, start_key, end_key);
      if (r.cnt > 0) {
        stats.min = r.min;
        stats.max = r.max;
        stats.sum = r.sum;
        stats.avg = r.sum / r.cnt;
        return;
      }
    } else if (interval_count >= kIntervalsPerTimeSpan) {
      TryAutoBuildSegmentTree();
      if (segment_tree_ready_.load(std::memory_order_acquire)) {
        const InternalStats r = SegmentTreeQuery(segment_tree_root_, start_key, end_key);
        if (r.cnt > 0) {
          stats.min = r.min;
          stats.max = r.max;
          stats.sum = r.sum;
          stats.avg = r.sum / r.cnt;
          return;
        }
      }
    }

    StatsOriginalImpl(start_key, end_key, stats);
  }

  //
  void Compaction(void) { container_mgr_.Compaction(); }
  inline void CompactionAndRebuildAsync(void) {
    container_mgr_.Compaction();
    TryAutoBuildSegmentTree();
  }
  inline void Reset(void) noexcept { FlatTsdIndex::thread_cache_ = {}; }

private:
  uint32_t tag_id_ = std::numeric_limits<uint32_t>::max();
  TimeSpanContainerMgr<kTimeSpanCapacity> container_mgr_;

  // Segment tree node
  struct alignas(64) SegmentTreeNode final {
    Timestamp start_time{0};
    Timestamp end_time{0};
    InternalStats stats{};
    uint32_t left_child{0};
    uint32_t right_child{0};
    uint32_t timespan_idx{0};
    uint16_t interval_id{0};
    bool is_leaf{false};
    bool is_valid{false};
  };

  // Segment tree storage
  std::vector<SegmentTreeNode> segment_tree_nodes_;
  uint32_t segment_tree_root_{0};
  alignas(64) std::atomic<bool> segment_tree_ready_{false};
  alignas(64) std::atomic<bool> segment_tree_building_{false};

  struct ThreadCache final {
    uint32_t tag_id = std::numeric_limits<uint32_t>::max();
    uint64_t last_interval_start = 0;
    uint64_t last_interval_end = 0;
    uint64_t last_timespan_start = 0;
    uint64_t last_timespan_end = 0;
    size_t last_interval_id = 0;
    TimeSpanContainer *last_timespan_container = nullptr;

    // Added: cache for second timespan
    uint64_t alt_timespan_start = 0;
    uint64_t alt_timespan_end = 0;
    TimeSpanContainer *alt_timespan_container = nullptr;
  };

  static thread_local ThreadCache thread_cache_;

  inline __attribute__((always_inline)) void
  CalculateContext(uint64_t key, uint64_t &timespan_start, size_t &interval_id,
                   TimeSpanContainer *&timespan_container) noexcept {
    ThreadCache &tc = thread_cache_;
    if (tc.tag_id != tag_id_)
      [[unlikely]] { goto rebuild_t; }

    // 1. Check if in cached interval
    if (key >= tc.last_interval_start && key <= tc.last_interval_end) {
      timespan_start = tc.last_timespan_start;
      interval_id = tc.last_interval_id;
      timespan_container = tc.last_timespan_container;
      return;
    }

    // 2. Check if in primary cache day range
    if (key >= tc.last_timespan_start && key <= tc.last_timespan_end) {
      const uint64_t time_in_timespan = key - tc.last_timespan_start;
      const size_t new_interval_id =
          (uint32_t)time_in_timespan / (uint32_t)kMsPerInterval;

      tc.last_interval_id = new_interval_id;
      tc.last_interval_start =
          tc.last_timespan_container->GetIntervalStart(new_interval_id);
      tc.last_interval_end = tc.last_interval_start + kMsPerIntervalMinus1;

      timespan_start = tc.last_timespan_start;
      interval_id = new_interval_id;
      timespan_container = tc.last_timespan_container;
      return;
    }

    // 3. Check if in alternate cache day range
    if (key >= tc.alt_timespan_start && key <= tc.alt_timespan_end) {
      // Promote alternate cache to primary cache
      std::swap(tc.last_timespan_start, tc.alt_timespan_start);
      std::swap(tc.last_timespan_end, tc.alt_timespan_end);
      std::swap(tc.last_timespan_container, tc.alt_timespan_container);

      const uint64_t time_in_timespan = key - tc.last_timespan_start;
      const size_t new_interval_id =
          (uint32_t)time_in_timespan / (uint32_t)kMsPerInterval;

      tc.last_interval_id = new_interval_id;
      tc.last_interval_start =
          tc.last_timespan_container->GetIntervalStart(new_interval_id);
      tc.last_interval_end = tc.last_interval_start + kMsPerIntervalMinus1;

      timespan_start = tc.last_timespan_start;
      interval_id = new_interval_id;
      timespan_container = tc.last_timespan_container;
      //++day_missed_count;
      return;
    }

  rebuild_t:
    // 4. Not in any cache, create new timespan container
    const uint64_t time_in_timespan = key % kMsPerTimeSpan;
    const uint64_t new_timespan_start = key - time_in_timespan;
    const uint64_t new_timespan_end = new_timespan_start + kMsPerTimeSpanMinus1;

    const size_t new_interval_id =
        (uint32_t)time_in_timespan / (uint32_t)kMsPerInterval;

    auto new_timespan_container = container_mgr_.Get(new_timespan_start);
    const uint64_t new_interval_start =
        new_timespan_container->GetIntervalStart(new_interval_id);
    const uint64_t new_interval_end = new_interval_start + kMsPerIntervalMinus1;

    // Demote current primary cache to alternate cache, new cache becomes primary
    tc.alt_timespan_start = tc.last_timespan_start;
    tc.alt_timespan_end = tc.last_timespan_end;
    tc.alt_timespan_container = tc.last_timespan_container;

    tc.tag_id = tag_id_;
    tc.last_timespan_start = new_timespan_start;
    tc.last_timespan_end = new_timespan_end;
    tc.last_timespan_container = new_timespan_container;
    tc.last_interval_id = new_interval_id;
    tc.last_interval_start = new_interval_start;
    tc.last_interval_end = new_interval_end;

    timespan_start = tc.last_timespan_start;
    interval_id = new_interval_id;
    timespan_container = tc.last_timespan_container;
    //++full_missed_count;
  }

  // Original statistics implementation
  void StatsOriginalImpl(Timestamp start_key, Timestamp end_key,
                         ExternalStats &stats) {
    // 1. Calculate all (timespan + interval) to be scanned
    std::vector<std::pair<uint64_t, uint16_t>> di_list;
    di_list.reserve(
        (end_key - start_key + kMsPerIntervalMinus1) / kMsPerInterval + 1);
    uint64_t curr_timespan;
    size_t curr_interval;
    TimeSpanContainer *curr_timespan_container;
    CalculateContext(start_key, curr_timespan, curr_interval,
                     curr_timespan_container);
    di_list.emplace_back(curr_timespan, static_cast<uint16_t>(curr_interval));

    while (true) {
      uint64_t next_timespan = curr_timespan;
      uint16_t next_interval = static_cast<uint16_t>(curr_interval + 1);
      if (next_interval >= kIntervalsPerTimeSpan) {
        next_timespan += kMsPerTimeSpan;
        next_interval = 0;
      }

      const Timestamp next_interval_start =
          next_timespan + static_cast<uint64_t>(next_interval) * kMsPerInterval;
      if (next_interval_start > end_key)
        break;

      di_list.emplace_back(next_timespan, next_interval);
      curr_timespan = next_timespan;
      curr_interval = next_interval;
    }

    std::vector<InternalStats> results;
    results.reserve(di_list.size());
    for (const auto & [ timespan_start, interval_id ] : di_list) {
      const Timestamp interval_start =
          timespan_start + static_cast<uint64_t>(interval_id) * kMsPerInterval;
      const Timestamp interval_end = interval_start + kMsPerIntervalMinus1;
      const Timestamp scan_start = std::max(start_key, interval_start);
      const Timestamp scan_end = std::min(end_key, interval_end);
      CalculateContext(scan_start, curr_timespan, curr_interval,
                       curr_timespan_container);
      results.emplace_back(
          curr_timespan_container->Stats(interval_id, scan_start, scan_end));
    }

    stats = {};
    uint64_t total_tuples = 0;
    for (const auto &r : results) {
      if (r.min < stats.min) {
        stats.min = r.min;
      }
      if (r.max > stats.max) {
        stats.max = r.max;
      }
      stats.sum += r.sum;
      total_tuples += r.cnt;
    }
    if (total_tuples > 0)
      stats.avg = stats.sum / total_tuples;
  }

  // Try to auto-build segment tree
  void TryAutoBuildSegmentTree() noexcept {
    bool expected = false;
    if (!segment_tree_ready_.load(std::memory_order_acquire) &&
        segment_tree_building_.compare_exchange_strong(expected, true,
                                                       std::memory_order_acq_rel)) {
      std::thread([this]() {
        BuildSegmentTree();
        segment_tree_building_.store(false, std::memory_order_release);
      }).detach();
    }
  }

  // Build segment tree
  void BuildSegmentTree() {
    std::vector<std::tuple<Timestamp, Timestamp, uint32_t, uint16_t, InternalStats>> leaves;
    leaves.reserve(1024);

    const size_t cap = container_mgr_.CapacityValue();
    for (size_t i = 0; i < cap; ++i) {
      TimeSpanContainer *container = container_mgr_.GetAt(i);
      if (!container) continue;

      for (uint16_t interval_id = 0; interval_id < kIntervalsPerTimeSpan; ++interval_id) {
        const Timestamp interval_start = container->GetIntervalStart(interval_id);
        const Timestamp interval_end = container->GetIntervalEnd(interval_id);

        if (container->GetIntervalState(interval_id) != IntervalStore::State::kClosedSorted) {
          continue;
        }
        InternalStats s = container->Stats(interval_id, interval_start, interval_end);
        if (s.cnt > 0) {
          leaves.emplace_back(interval_start, interval_end, static_cast<uint32_t>(i), interval_id, s);
        }
      }
    }

    if (leaves.empty()) {
      segment_tree_ready_.store(false, std::memory_order_release);
      return;
    }

    std::sort(leaves.begin(), leaves.end(),
              [](const auto &a, const auto &b) { return std::get<0>(a) < std::get<0>(b); });

    segment_tree_nodes_.clear();
    segment_tree_nodes_.reserve(leaves.size() * 2);
    segment_tree_root_ = BuildSegmentTreeRecursive(leaves, 0, leaves.size() - 1);
    segment_tree_ready_.store(true, std::memory_order_release);
  }

  // Recursive build
  uint32_t BuildSegmentTreeRecursive(
      const std::vector<std::tuple<Timestamp, Timestamp, uint32_t, uint16_t, InternalStats>> &leaves,
      size_t left, size_t right) {
    if (left == right) {
      segment_tree_nodes_.emplace_back();
      auto &node = segment_tree_nodes_.back();
      node.start_time = std::get<0>(leaves[left]);
      node.end_time = std::get<1>(leaves[left]);
      node.stats = std::get<4>(leaves[left]);
      node.timespan_idx = std::get<2>(leaves[left]);
      node.interval_id = std::get<3>(leaves[left]);
      node.left_child = node.right_child = 0;
      node.is_leaf = true;
      node.is_valid = true;
      return static_cast<uint32_t>(segment_tree_nodes_.size() - 1);
    }

    size_t mid = left + (right - left) / 2;
    uint32_t lch = BuildSegmentTreeRecursive(leaves, left, mid);
    uint32_t rch = BuildSegmentTreeRecursive(leaves, mid + 1, right);

    segment_tree_nodes_.emplace_back();
    auto &node = segment_tree_nodes_.back();
    const auto &ln = segment_tree_nodes_[lch];
    const auto &rn = segment_tree_nodes_[rch];
    node.start_time = ln.start_time;
    node.end_time = rn.end_time;
    node.stats.min = std::min(ln.stats.min, rn.stats.min);
    node.stats.max = std::max(ln.stats.max, rn.stats.max);
    node.stats.sum = ln.stats.sum + rn.stats.sum;
    node.stats.cnt = ln.stats.cnt + rn.stats.cnt;
    node.left_child = lch;
    node.right_child = rch;
    node.is_leaf = false;
    node.is_valid = true;
    return static_cast<uint32_t>(segment_tree_nodes_.size() - 1);
  }

  // Segment tree query
  InternalStats SegmentTreeQuery(uint32_t node_idx, Timestamp start,
                                 Timestamp end) const noexcept {
    if (segment_tree_nodes_.empty()) return {};
    const SegmentTreeNode &node = segment_tree_nodes_[node_idx];
    if (!node.is_valid) return {};

    // Disjoint
    if (node.end_time < start || node.start_time > end) {
      return {};
    }

    // Fully contained
    if (node.start_time >= start && node.end_time <= end) {
      return node.stats;
    }

    // Leaf node partial overlap
    if (node.is_leaf) {
      return GetPreciseIntervalStats(node, start, end);
    }

    InternalStats left_stats = SegmentTreeQuery(node.left_child, start, end);
    InternalStats right_stats = SegmentTreeQuery(node.right_child, start, end);
    InternalStats merged{};
    if (left_stats.cnt == 0) return right_stats;
    if (right_stats.cnt == 0) return left_stats;
    merged.min = std::min(left_stats.min, right_stats.min);
    merged.max = std::max(left_stats.max, right_stats.max);
    merged.sum = left_stats.sum + right_stats.sum;
    merged.cnt = left_stats.cnt + right_stats.cnt;
    return merged;
  }

  // Leaf node precise query
  InternalStats GetPreciseIntervalStats(const SegmentTreeNode &node,
                                        Timestamp start,
                                        Timestamp end) const noexcept {
    TimeSpanContainer *container = container_mgr_.GetAt(node.timespan_idx);
    if (!container) return {};
    const Timestamp actual_start = std::max(start, node.start_time);
    const Timestamp actual_end = std::min(end, node.end_time);
    return container->Stats(node.interval_id, actual_start, actual_end);
  }
};

// Thread-local cache static initialization
thread_local FlatTsdIndex::ThreadCache FlatTsdIndex::thread_cache_{};
