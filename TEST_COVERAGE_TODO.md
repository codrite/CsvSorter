# Test Coverage Improvement TODO

## High Priority

### SplitCsvFile — Unit tests for core sorting logic
- [x] Add unit tests for `Line.compareTo()` with various string orderings
- [x] Add unit tests for `Line.equals()` and `Line.hashCode()`
- [x] Verify output chunks are actually sorted after `execute()`
- [ ] Test with an empty input file
- [x] Test with a single-line input file
- [x] Test with fewer rows than `maxRecordsInMemory`
- [x] Test with duplicate values in the sort column
- [x] Test sorting by different column index
- [ ] Test CSV values containing commas and quotes
- [ ] Test with an out-of-range index field to document `ArrayIndexOutOfBoundsException` behavior

### Merge2Files — Enable and expand merge tests
- [x] Enable the disabled `shouldMerge2Files()` test using `@TempDir` for dynamic file creation
- [x] Test merging two sorted files and verify output order
- [x] Test merging files of unequal length
- [ ] Test merging an empty file with a non-empty file
- [x] Test merging files with duplicate keys
- [x] Test behavior when input files don't exist
- [x] Test `run()` delegates to `execute()` correctly
- [x] Test `isGreater()` with equal values

### MergeFiles — Add assertions and edge cases
- [ ] Add assertions to existing test to verify final output file exists and is correctly sorted
- [ ] Test with a single input file (should copy without merging)
- [ ] Test with an odd number of input files (unpaired file in a merge round)
- [ ] Verify staging directory cleanup after execution

## Medium Priority

### CsvFileSorterApplication — Input validation and directory handling
- [ ] Test `prepareFolders()` creates and cleans the staging directory
- [ ] Test with missing CLI arguments
- [ ] Test with a non-existent input file path
- [ ] Test with a non-numeric index field argument
- [ ] Test full end-to-end flow through `main()` with valid arguments

### Concurrency correctness
- [ ] Stress test concurrent merge execution with many threads and files
- [ ] Verify `CountDownLatch` synchronization under parallel load
- [ ] Test with concurrency level greater than number of file pairs

## Low Priority

### Error and resilience paths
- [ ] Test behavior with unreadable input files (permission errors)
- [ ] Test with malformed CSV input (missing columns, inconsistent column counts)
- [ ] Verify temporary file cleanup after errors
- [ ] Test resource cleanup when exceptions occur mid-execution
