#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <cppconn/resultset.h>
#include <mutex>
#include <vector>
#include <memory>
#include <condition_variable>
#include <chrono>

class MysqlPool {
public:
    static MysqlPool& getInstance() {
        static MysqlPool _instance;
        return _instance;
    }

    bool init(const std::string& host, const std::string& user, const std::string& pwd, const std::string& db_name, int size = 10) {
        try {
            std::unique_ptr<sql::Connection> tmp_conn(
                sql::mysql::get_mysql_driver_instance()->connect(host, user, pwd)
            );

            std::unique_ptr<sql::Statement> stmt(tmp_conn->createStatement());
            stmt->execute("CREATE DATABASE IF NOT EXISTS " + db_name);
            
            tmp_conn->setSchema(db_name);

            stmt->execute("CREATE TABLE IF NOT EXISTS users ("
                        "id VARCHAR(255) PRIMARY KEY, "
                        "pwd VARCHAR(255), "
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");

            stmt->execute("CREATE TABLE IF NOT EXISTS reservations ("
                        "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                        "user_id VARCHAR(255) NOT NULL, "
                        "time DATETIME NOT NULL, "
                        "room VARCHAR(255) NOT NULL, "
                        "meeting_type VARCHAR(32) NOT NULL DEFAULT 'reserved', "
                        "status VARCHAR(32) NOT NULL DEFAULT 'scheduled', "
                        "ended_at DATETIME NULL, "
                        "end_reason VARCHAR(64) NULL, "
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                        "INDEX idx_reservations_user_id (user_id), "
                        "INDEX idx_reservations_room (room), "
                        "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)");

                    // Backward-compatible schema migration for older deployments.
                    ensure_column_exists(tmp_conn.get(), db_name, "reservations", "meeting_type",
                             "ALTER TABLE reservations ADD COLUMN meeting_type VARCHAR(32) NOT NULL DEFAULT 'reserved'");
                    ensure_column_exists(tmp_conn.get(), db_name, "reservations", "status",
                             "ALTER TABLE reservations ADD COLUMN status VARCHAR(32) NOT NULL DEFAULT 'scheduled'");
                    ensure_column_exists(tmp_conn.get(), db_name, "reservations", "ended_at",
                             "ALTER TABLE reservations ADD COLUMN ended_at DATETIME NULL");
                    ensure_column_exists(tmp_conn.get(), db_name, "reservations", "end_reason",
                             "ALTER TABLE reservations ADD COLUMN end_reason VARCHAR(64) NULL");
                    ensure_index_exists(tmp_conn.get(), db_name, "reservations", "idx_reservations_room",
                            "ALTER TABLE reservations ADD INDEX idx_reservations_room (room)");

            std::lock_guard<std::mutex> lock(_mutex);
            for (int i = 0; i < size; ++i) {
                sql::Connection* conn = sql::mysql::get_mysql_driver_instance()->connect(host, user, pwd);
                conn->setSchema(db_name); 
                _pool.push_back(conn);
            }
            
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Init Error: " << e.what() << std::endl;
            return false;
        }
        return true;
    }



    std::unique_ptr<sql::Connection, std::function<void(sql::Connection*)>> getConnection() {
        std::unique_lock<std::mutex> lock(_mutex);
        if (!_cond.wait_for(lock, std::chrono::milliseconds(500), [this] { return !_pool.empty(); })) {
            return nullptr; // Timeout or no available connections
        }
        auto conn = _pool.back();
        _pool.pop_back();
        return std::unique_ptr<sql::Connection, std::function<void(sql::Connection*)>>(conn, [this](sql::Connection* c) {
            std::lock_guard<std::mutex> lock(_mutex);
            _pool.emplace_back(c);
            _cond.notify_one();
        });
    }

    
private:
    static bool column_exists(sql::Connection* conn, const std::string& db_name,
                              const std::string& table, const std::string& column) {
        std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
            "SELECT 1 FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1"));
        stmt->setString(1, db_name);
        stmt->setString(2, table);
        stmt->setString(3, column);
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
        return res->next();
    }

    static bool index_exists(sql::Connection* conn, const std::string& db_name,
                             const std::string& table, const std::string& index_name) {
        std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
            "SELECT 1 FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1"));
        stmt->setString(1, db_name);
        stmt->setString(2, table);
        stmt->setString(3, index_name);
        std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
        return res->next();
    }

    static void ensure_column_exists(sql::Connection* conn, const std::string& db_name,
                                     const std::string& table, const std::string& column,
                                     const std::string& alter_sql) {
        if (column_exists(conn, db_name, table, column)) {
            return;
        }
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        stmt->execute(alter_sql);
    }

    static void ensure_index_exists(sql::Connection* conn, const std::string& db_name,
                                    const std::string& table, const std::string& index_name,
                                    const std::string& alter_sql) {
        if (index_exists(conn, db_name, table, index_name)) {
            return;
        }
        std::unique_ptr<sql::Statement> stmt(conn->createStatement());
        stmt->execute(alter_sql);
    }

    MysqlPool() = default;

    ~MysqlPool() {
        for (auto& conn : _pool) {
            delete conn;
        }
    }
    MysqlPool(const MysqlPool&) = delete;
    MysqlPool& operator=(const MysqlPool&) = delete;
    MysqlPool(MysqlPool&&) = delete;
    MysqlPool& operator=(MysqlPool&&) = delete;

    std::mutex _mutex;
    std::vector<sql::Connection*> _pool;
    std::condition_variable _cond;
};