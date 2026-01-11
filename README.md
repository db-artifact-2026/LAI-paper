An Anonymous High-Performance Time-Series Index Prototype

This repository provides the implementation and evaluation code for a
lock-free, lifecycle-aware in-memory time-series indexing system.
The artifacts are provided to support anonymous review and reproducibility.

Core Features
- Lock-Free Concurrency: CAS-based lock-free write buffer for highly concurrent ingestion
- Adaptive Tiering: Multi-level storage organization with automatic data demotion
- Batch Optimization: Batched appends to reduce synchronization overhead
- Efficient Queries: Interpolation search with pivot sampling
- Range Aggregation: Segment-tree-based aggregation over sorted segments

Build & Run

Build:
g++ -std=c++2a -march=native -mavx2 LAI_benchmark.cpp -o benchmark -O3 -lpthread

Run:
./benchmark <number_of_threads>

Notes:
- This repository is anonymized for double-blind review.
- No identifying information about the authors or institutions is included.
