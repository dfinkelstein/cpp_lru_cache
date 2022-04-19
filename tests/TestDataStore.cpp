#include <string>
#include <gtest/gtest.h>

#include "DataStore.h"

TEST(TestDataStore, TestPut)
{
    DataStore ds = DataStore(3, "PutTest.db");

    ds.put("1", "one");
    ASSERT_EQ(ds.isInCache("1"), true);
    ASSERT_EQ(ds.size(), 1);

    ds.put("2", "two");
    ASSERT_EQ(ds.isInCache("2"), true);
    ASSERT_EQ(ds.size(), 2);

    ds.put("3", "three");
    ASSERT_EQ(ds.isInCache("3"), true);
    ASSERT_EQ(ds.size(), 3);

}

TEST(TestDataStore, TestPutUpdate)
{
    DataStore ds = DataStore(1, "UpdateTest.db");
    
    // Test basic put
    ds.put("1", "one");
    ASSERT_EQ(ds.size(), 1);
    ASSERT_EQ(ds.isInCache("1"), true);
    EXPECT_EQ(ds.get("1"), "one");

    // Test update
    ds.put("1", "numberone");
    ASSERT_EQ(ds.size(), 1);
    ASSERT_EQ(ds.isInCache("1"), true);
    EXPECT_EQ(ds.get("1"), "numberone");
}

TEST(TestDataStore, TestMaxCacheSize)
{
    DataStore ds = DataStore(1, "CacheTest.db");

    ds.put("1", "one");
    EXPECT_EQ(ds.size(), 1);

    ds.put("2", "two"); // This should push the first value out of the cache
    EXPECT_EQ(ds.size(), 1);

    ASSERT_EQ(ds.isInCache("1"), false);

    EXPECT_EQ(ds.get("1"), "one");

    EXPECT_EQ(ds.isInCache("2"), false);
}
