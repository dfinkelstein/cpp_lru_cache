#ifndef _DATASTORE_
#define _DATASTORE_

#include <iostream>
#include <sstream>
#include <list>
#include <unordered_map>
#include <sqlite3.h> 

/**
 * @brief A data storage class utilizing an LRU Cache in front of a sqlite database. \n 
 * 
 * Note: Currently only supports storing strings, so object serialization is up to the user.
 * 
 */
class DataStore
{
public:
    typedef std::pair<std::string, std::string> key_val_pair;
    typedef std::list<key_val_pair>::iterator list_itr;

    /**
     * @brief Construct a new Data Store object
     * 
     * @param max_cache_size The maximum size of the LRU cache
     * @param dataStoreName The name to use for the sqlite database (defaults to "DataStore.db")
     */
    DataStore(size_t max_cache_size, std::string dataStoreName = "DataStore.db") :
        m_max_cache_size(max_cache_size) 
    {
        // Initialize database
        int status = sqlite3_open(dataStoreName.c_str(), &m_db);
        if (status)
        {
            throw std::runtime_error("Failed to open database: " + std::string(sqlite3_errmsg(m_db)));
        }

        // Create the table in the database to store the values (but only if it does not already exist)
        char* errMsg = nullptr;
        const char* createTblSql = "CREATE TABLE IF NOT EXISTS data (key CHAR PRIMARY KEY, value TEXT)";
        status = sqlite3_exec(m_db, createTblSql, NULL, nullptr, &errMsg);
        if (status != SQLITE_OK)
        {
            std::string error(errMsg);
            sqlite3_free(errMsg);
            throw std::runtime_error("SQL error ocurred: " + error);
        }
    }

    /**
     * @brief Destroy the Data Store object
     */
    ~DataStore()
    {
        // Purge the cache to persistent storage
        if (!m_cache_list.empty())
        {
            purgeToStorage();
        }
        // Close out database and clean up memory
        sqlite3_close(m_db);
    }

    /**
     * @brief Store a value into the data store
     * 
     * @param key Key to reference item by
     * @param value Value to store
     */
    void put(const std::string& key, const std::string& value)
    {
        // Look for the item in the cache
        auto mapItr = m_cache_map.find(key);

        // Add the item to the history list at the front (because it was just accessed)
        m_cache_list.push_front(key_val_pair(key, value));

        if (mapItr != m_cache_map.end())
        {
            // Remove it from it's old position in the list if it existed
            m_cache_list.erase(mapItr->second);
        }

        // Add the list location to the cache
        m_cache_map[key] = m_cache_list.begin();

        // Mark it as modified
        m_modification_map[key] = true;

        // If we are exceeding the size of the cache, we need to purge the oldest 
        // element to the persistent store
        if (m_cache_map.size() > m_max_cache_size)
        {
            // Get the last element in the history list (least recently used)
            // and remove it from the cache.
            auto end_itr = m_cache_list.rbegin();
            m_cache_map.erase(end_itr->first);
            m_modification_map.erase(end_itr->first);

            // Save the data and remove it from the history list
            key_val_pair lastElem = m_cache_list.back();
            m_cache_list.pop_back();

            // Write the data to the persistent store, only if it has
            // been modified
            if (isModified(lastElem.first))
            {
                writeToDB(lastElem.first, lastElem.second);
            }
        }
    }

    /**
     * @brief Get a value from the data store
     * 
     * @param key The key to retrieve
     * @return std::string The stored value or empty string if the key does not exist
     */
    std::string get(const std::string& key)
    {
        // Look for the item in the cache
        auto mapItr = m_cache_map.find(key);
        if (mapItr != m_cache_map.end())
        {
            // If it exists, move it to the front of the history list and return the value
            m_cache_list.splice(m_cache_list.begin(), m_cache_list, mapItr->second);
            return mapItr->second->second;
        }
        else 
        {
            // Cache miss, get value from persistent store and put it in cache
            std::string value;
            bool success = readFromDB(key, value);
            if (success)
            {
                // If we succesfully retrieved the value, put it into the cache as the 
                // most recently accessed item. Then, get that item from the cache
                // and return the value
                put(key, value);
                // Since we just retrieved it from the database, it isnt really modified, yet
                m_modification_map[key] = false; 

                mapItr = m_cache_map.find(key);
                if (mapItr != m_cache_map.end())
                {
                    return mapItr->second->second;
                }
            }
            // If unsucesfful, the readFromDB function should print out the error that
            // occurred.
        }

        // Return an empty string if we did not find the key
        return "";
    }

    /**
     * @brief Checks to see if the provided value exists in the cache. \n
     * Note: this does not check if it exists in the persistent storage
     * 
     * @param key The key to check for
     * @return true if the key is in the cache
     * @return false if the key is not in the cache
     */
    bool isInCache(const std::string& key)
    {
        return m_cache_map.find(key) != m_cache_map.end();
    }

    /**
     * @brief Gets the current size of the cache
     * 
     * @return size_t The current size of the cache
     */
    size_t size()
    {
        return m_cache_map.size();
    }

private:
    std::list<key_val_pair> m_cache_list;
    std::unordered_map<std::string, list_itr> m_cache_map;
    std::unordered_map<std::string, bool> m_modification_map; 

    size_t m_max_cache_size;

    sqlite3 *m_db;

    /**
     * @brief Writes a value to the persistent store
     * 
     * @param key The key to write
     * @param value The value to save
     * @return true If the write was successful
     * @return false If the write failed
     */
    bool writeToDB(const std::string& key, const std::string& value)
    {
        std::stringstream ss;
        ss << "INSERT OR REPLACE INTO data (key, value) VALUES ( '" << key << "', '" << value << "' );";

        char* errMsg = nullptr;
        int status = sqlite3_exec(m_db, ss.str().c_str(), NULL, nullptr, &errMsg);
        if (status != SQLITE_OK)
        {
            std::cerr << "SQL error ocurred: " << std::string(errMsg) << std::endl;
            sqlite3_free(errMsg);
            return false;
        }

        return true;
    }

    /**
     * @brief Retrieves a value from the persistant store
     * 
     * @param key The key to retrieve
     * @param value The value that was retrieved
     * @return true If the retrieve was successful
     * @return false If the retrieve failed
     */
    bool readFromDB(const std::string& key, std::string& value)
    {
        std::stringstream ss;
        ss << "SELECT value FROM data WHERE key = '" << key << "' LIMIT 1;";

        // Created the prepared SQL statement
        sqlite3_stmt *stmt;
        int status = sqlite3_prepare_v2(m_db, ss.str().c_str(), -1, &stmt, NULL);
        if (status != SQLITE_OK) {
            std::cerr << "SQL error ocurred: " << std::string(sqlite3_errmsg(m_db));
            sqlite3_finalize(stmt);
            return false;
        }

        // Execute the statement
        while ((status = sqlite3_step(stmt)) == SQLITE_ROW) {
            value = std::string((char *)sqlite3_column_text(stmt, 0));
        }
        if (status != SQLITE_DONE) {
            std::cerr << "SQL error ocurred: " << std::string(sqlite3_errmsg(m_db));
            sqlite3_finalize(stmt);
            return false;
        }

        // Clean up after executing the statment
        sqlite3_finalize(stmt);

        return true;
    }

    /**
     * @brief Purges the remaining elements in the cache to the persistent storage
     * 
     * @return true If purge was succesful
     * @return false If purge failed
     */
    bool purgeToStorage()
    {
        bool dataAdded = false;

        std::stringstream ss;
        ss << "INSERT OR REPLACE INTO data (key, value) VALUES ";
        for (auto listItr = m_cache_list.begin(); listItr != m_cache_list.end(); ++listItr)
        {
            // Only write it back out to the persistent storage if it has been modified
            if (isModified(listItr->first))
            {
                ss << "( '" << listItr->first << "', '" << listItr->second << "' ),";
                dataAdded = true;
            }
        }
        ss.seekp(-1, std::ios_base::end); // Remove the extra comma for the last value
        ss << ";"; // Add the semicolon to finish the SQL statement

        // Only perform the write if there is data to write
        if (dataAdded)
        {
            char* errMsg = nullptr;
            int status = sqlite3_exec(m_db, ss.str().c_str(), NULL, nullptr, &errMsg);
            if (status != SQLITE_OK)
            {
                std::cerr << "SQL error ocurred: " << std::string(errMsg) << std::endl;
                sqlite3_free(errMsg);
                return false;
            }
        }

        return true;
    }

    bool isModified(std::string key)
    {
        auto mapItr = m_modification_map.find(key);
        if (mapItr != m_modification_map.end())
        {
            return mapItr->second;
        }

        return false;
    }
};

#endif /* _DATASTORE_ */