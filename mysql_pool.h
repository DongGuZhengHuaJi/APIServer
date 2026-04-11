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
                        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                        "INDEX idx_reservations_user_id (user_id), "
                        "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)");

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