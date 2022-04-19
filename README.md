# C++ Data Store with LRU Cache
This is an example of a data storage class supplemented by an in-memory LRU cache. The persistent storage backing is a sqlite database.

## Tests
The tests for this implementation are done with GoogleTest

## Building and Running
```bash
mkdir build
cd build
cmake ../
make
ctest
# or 
./TestDataStore
```
## Further improvements
- [ ] Template the class to allow for more generic storage
- [ ] Add more throrough testing
    - [ ] Check database to ensure the correct values wre written
