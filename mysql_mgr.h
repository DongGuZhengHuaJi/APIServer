#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <iostream>
#include <cstdlib>
#include "mysql_pool.h"

struct ReserveMeetingInfo {
    std::string from;
    std::chrono::system_clock::time_point time;
    std::string room;

    ReserveMeetingInfo(const std::string& f, const std::chrono::system_clock::time_point& t, const std::string& r)
        : from(f), time(t), room(r) {}
};

class MysqlManager {
public:
    // static MysqlManager& getInstance() {
    //     static MysqlManager _instance;
    //     return _instance;
    // }

    static bool initPool(const std::string& host, const std::string& user, const std::string& pwd, const std::string& db, int size = 10) {
        //初始化MySQL数据库和用户表
        MysqlPool& pool = MysqlPool::getInstance();
        if (!pool.init(host, user, pwd, db, size)) {
            std::cerr << "Failed to initialize MySQL connection pool" << std::endl;
            return false;
        }
        return true;
    }

    static bool initPoolFromEnv() {
        const std::string host = env_or_default("MYSQL_HOST", "127.0.0.1");
        const int port = env_int_or_default("MYSQL_PORT", 3306);
        const std::string user = env_or_default("MYSQL_USER", "root");
        const std::string password = env_or_default("MYSQL_PASSWORD", "");
        const std::string db = env_or_default("MYSQL_DB", "meeting_db");
        const int pool_size = env_int_or_default("MYSQL_POOL_SIZE", 10);

        std::string endpoint = host;
        if (host.rfind("tcp://", 0) != 0 && host.rfind("unix://", 0) != 0) {
            endpoint = "tcp://" + host + ":" + std::to_string(port);
        }

        if (!initPool(endpoint, user, password, db, pool_size)) {
            std::cerr << "MySQL env init failed."
                      << " host=" << host
                      << " port=" << port
                      << " user=" << user
                      << " db=" << db
                      << " pool_size=" << pool_size
                      << std::endl;
            return false;
        }

        std::cout << "MySQL initialized from env."
                  << " host=" << host
                  << " port=" << port
                  << " user=" << user
                  << " db=" << db
                  << " pool_size=" << pool_size
                  << std::endl;
        return true;
    }
    
    static bool checkUserExists(const std::string& id){
        //检查用户是否存在在mysql中
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement("SELECT id FROM users WHERE id = ?"));
            stmt->setString(1, id);
            std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
            return res->next();
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Query Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool addUser(const std::string& id, const std::string& pwd){
        //添加用户到mysql
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement("INSERT INTO users (id, pwd) VALUES (?, ?)"));
            stmt->setString(1, id);
            stmt->setString(2, pwd);
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Insert Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool checkPwd(const std::string& id, const std::string& pwd){
        //检查用户密码是否正确
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement("SELECT pwd FROM users WHERE id = ?"));
            stmt->setString(1, id);
            std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
            if (res->next()) {
                return res->getString("pwd") == pwd;
            }
            return false;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Query Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool addReservation(const ReserveMeetingInfo& info) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement("INSERT INTO reservations (user_id, time, room) VALUES (?, ?, ?)"));
            stmt->setString(1, info.from);
            stmt->setString(2, to_mysql_datetime(info.time));
            stmt->setString(3, info.room);
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Insert Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool removeReservation(const ReserveMeetingInfo& info) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "DELETE FROM reservations WHERE user_id = ? AND time = ? AND room = ?"));
            stmt->setString(1, info.from);
            stmt->setString(2, to_mysql_datetime(info.time));
            stmt->setString(3, info.room);
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Delete Error: " << e.what() << std::endl;
            return false;
        }
    }

private:
    static std::string to_mysql_datetime(const std::chrono::system_clock::time_point& tp) {
        const std::time_t tt = std::chrono::system_clock::to_time_t(tp);
        std::tm tm_buf {};
        localtime_r(&tt, &tm_buf);

        std::ostringstream oss;
        oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    static std::string env_or_default(const char* key, const std::string& default_value) {
        const char* value = std::getenv(key);
        if (value == nullptr || value[0] == '\0') {
            return default_value;
        }
        return value;
    }

    static int env_int_or_default(const char* key, int default_value) {
        const char* value = std::getenv(key);
        if (value == nullptr || value[0] == '\0') {
            return default_value;
        }
        try {
            return std::stoi(value);
        } catch (...) {
            return default_value;
        }
    }
};