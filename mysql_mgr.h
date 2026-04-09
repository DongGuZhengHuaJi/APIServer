#include <string>
#include <iostream>
#include <mysql_pool.h>

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
};