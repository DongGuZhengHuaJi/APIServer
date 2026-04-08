#pragma once

#include <boost/beast.hpp>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <queue>
#include <string>
#include <unordered_map>
#include <thread>
#include <boost/asio.hpp>
#include "redis_mgr.h"
#include "mysql_mgr.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace beast = boost::beast;
namespace http = boost::beast::http;

class Session;

struct LogicSystem_Task {
    http::request<http::string_body> req;
    std::shared_ptr<Session> session;

    LogicSystem_Task(http::request<http::string_body> r, std::shared_ptr<Session> s);
};

class LogicSystem {
public:
    static LogicSystem& getInstance();

    void enqueueTask(LogicSystem_Task task);
    void processTasks();
    void handle_get_request(const http::request<http::string_body>& req, std::shared_ptr<Session> session);
    void handle_post_request(const http::request<http::string_body>& req, std::shared_ptr<Session> session);
    void register_get_handler();
    void register_post_handler();
    void stop();

private:
    LogicSystem();
    ~LogicSystem() = default;
    LogicSystem(const LogicSystem&) = delete;
    LogicSystem& operator=(const LogicSystem&) = delete;
    LogicSystem(LogicSystem&&) = delete;
    LogicSystem& operator=(LogicSystem&&) = delete;

private:
    std::atomic<bool> b_stop{false};
    std::unordered_map<std::string, std::function<void(std::shared_ptr<Session>)>> get_handlers;
    std::unordered_map<std::string, std::function<void(const nlohmann::json&, std::shared_ptr<Session>)>> post_handlers;
    std::queue<LogicSystem_Task> _message_queue;
    std::mutex _queue_mutex;
    std::condition_variable _cond;
};