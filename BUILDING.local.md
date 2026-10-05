# Native build and baseline verification

Use native entry points below. Build tooling and test selection are distinct from product requirements. Target task-specific cases and nearby regressions, record selected/filtered counts and exit codes. Zero selected tests is not success.

Use /home/sl_chen/.local/bin/cmake if CMake is not on PATH; fmt is upstream bundled, Catch2 is pinned to 53d0d913a422d356b23dd927547febdf69ee9081.

```sh
cmake -S . -B build -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DSPDLOG_BUILD_TESTS=ON -DSPDLOG_BUILD_EXAMPLE=ON
cmake --build build -j2
./build/tests/spdlog-utests --list-tests
./build/tests/spdlog-utests
```
