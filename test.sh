# B+ Tree test
cd build
# make b_plus_tree_insert_test -j$(nproc)
# ./test/b_plus_tree_insert_test
# make b_plus_tree_delete_test -j$(nproc)
# ./test/b_plus_tree_delete_test
make b_plus_tree_concurrent_test.cpp -j$(nproc)
./test/b_plus_tree_concurrent_test.cpp

