#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <iostream>
#include <cstdlib>
#include "mysql_pool.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;

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

    static bool addUser(const std::string& id, const std::string& pwd, const std::string& self_name = ""){
        //添加用户到mysql
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement("INSERT INTO users (id, pwd, self_name) VALUES (?, ?, ?)"));
            stmt->setString(1, id);
            stmt->setString(2, pwd);
            stmt->setString(3, self_name.empty() ? id : self_name);
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

    static std::string getUserName(const std::string& id) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return id;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "SELECT self_name FROM users WHERE id = ?"));
            stmt->setString(1, id);
            std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());
            if (!res->next()) {
                return id;
            }

            const std::string name = res->isNull("self_name") ? "" : res->getString("self_name");
            return name.empty() ? id : name;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Query Error: " << e.what() << std::endl;
            return id;
        }
    }

    static bool updateUserName(const std::string& id, const std::string& self_name) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "UPDATE users SET self_name = ? WHERE id = ?"));
            stmt->setString(1, self_name.empty() ? id : self_name);
            stmt->setString(2, id);
            return stmt->executeUpdate() > 0;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Update Error: " << e.what() << std::endl;
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
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "INSERT INTO meetings (user_id, time, room, meeting_type, status) VALUES (?, ?, ?, ?, ?)"));
            stmt->setString(1, info.from);
            stmt->setString(2, to_mysql_datetime(info.time));
            stmt->setString(3, info.room);
            stmt->setString(4, "reserved");
            stmt->setString(5, "scheduled");
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Insert Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool addQuickMeeting(const std::string& user_id, const std::string& room,
                                const std::chrono::system_clock::time_point& start_time) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "INSERT INTO meetings (user_id, time, room, meeting_type, status) VALUES (?, ?, ?, ?, ?)"));
            stmt->setString(1, user_id);
            stmt->setString(2, to_mysql_datetime(start_time));
            stmt->setString(3, room);
            stmt->setString(4, "quick");
            stmt->setString(5, "started");
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Insert QuickMeeting Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool addScreenShareMeeting(const std::string& user_id, const std::string& room,
                                const std::chrono::system_clock::time_point& start_time) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "INSERT INTO meetings (user_id, time, room, meeting_type, status) VALUES (?, ?, ?, ?, ?)"));
            stmt->setString(1, user_id);
            stmt->setString(2, to_mysql_datetime(start_time));
            stmt->setString(3, room);
            stmt->setString(4, "screen_share");
            stmt->setString(5, "started");
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Insert ScreenShare Error: " << e.what() << std::endl;
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
                "DELETE FROM meetings WHERE user_id = ? AND time = ? AND room = ?"));
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

    static bool closeMeeting(const std::string& room, const std::string& reason,
                                       const std::chrono::system_clock::time_point& closed_time) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "UPDATE meetings "
                "SET status = 'closed', ended_at = ?, end_reason = ? "
                "WHERE room = ? AND status != 'closed'"));
            stmt->setString(1, to_mysql_datetime(closed_time));
            stmt->setString(2, reason);
            stmt->setString(3, room);
            stmt->execute();
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Update Error: " << e.what() << std::endl;
            return false;
        }
    }

    static bool getUserMeetings(const std::string& user_id, json &reservations) {
        MysqlPool& pool = MysqlPool::getInstance();
        auto conn = pool.getConnection();
        if (!conn) {
            std::cerr << "Failed to get MySQL connection" << std::endl;
            return false;
        }

        try {
            std::unique_ptr<sql::PreparedStatement> stmt(conn->prepareStatement(
                "SELECT time, room, meeting_type, status, ended_at, end_reason "
                "FROM meetings WHERE user_id = ? ORDER BY time DESC"));
            stmt->setString(1, user_id);
            std::unique_ptr<sql::ResultSet> res(stmt->executeQuery());

            reservations = json::array();
            
            std::vector<std::string> to_update;
            while (res->next()) {
                const std::string time_str = res->getString("time");
                const std::string room = res->getString("room");
                const std::string meeting_type = res->getString("meeting_type");
                std::string status = res->getString("status");
                const std::string ended_at = res->isNull("ended_at") ? "" : res->getString("ended_at");
                const std::string end_reason = res->isNull("end_reason") ? "" : res->getString("end_reason");
                
                if(status == "scheduled" && !ended_at.empty()) {
                    //如果状态是scheduled但ended_at不为空，说明会议已经结束但状态未更新，修正状态为closed
                    to_update.push_back(room);
                    status = "closed";
                }

                reservations.push_back({
                    {"time", time_str},
                    {"room", room},
                    {"meeting_type", meeting_type},
                    {"status", status},
                    {"ended_at", ended_at},
                    {"end_reason", end_reason}
                });
            }
            // 更新状态为 closed 的会议
            for (const auto& room : to_update) {
                closeMeeting(room, "auto_closed", std::chrono::system_clock::now());
            }
            return true;
        } catch (sql::SQLException& e) {
            std::cerr << "MySQL Query Error: " << e.what() << std::endl;
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