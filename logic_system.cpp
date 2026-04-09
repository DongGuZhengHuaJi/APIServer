#include "logic_system.h"
#include "api_server.h"
#include "thread_pool.hpp"
#include "redis_mgr.h"
#include <nlohmann/json.hpp>


namespace http = boost::beast::http;
using json = nlohmann::json;

namespace {
constexpr int kAccessTokenExpireSeconds = 15 * 60;
constexpr int kRefreshTokenExpireSeconds = 7 * 24 * 60 * 60;

std::string access_token_key(const std::string& token) {
    return "access:" + token;
}

std::string refresh_token_key(const std::string& token) {
    return "refresh:" + token;
}

void send_json_response(std::shared_ptr<Session> session, http::status status, const json& rsp_json, const char* content_type) {
    http::response<http::string_body> rsp{status, 11};
    rsp.set(http::field::content_type, content_type);
    rsp.body() = rsp_json.dump();
    rsp.prepare_payload();
    session->send_response(rsp);
}
}  // namespace

LogicSystem_Task::LogicSystem_Task(http::request<http::string_body> r, std::shared_ptr<Session> s)
    : req(std::move(r)), session(std::move(s)) {}

LogicSystem& LogicSystem::getInstance() {
    static LogicSystem _instance;
    return _instance;
}

void LogicSystem::enqueueTask(LogicSystem_Task task) {
    std::lock_guard<std::mutex> lock(_queue_mutex);
    _message_queue.push(std::move(task));
    _cond.notify_one();
}

void LogicSystem::processTasks() {
    while (!b_stop.load()) {
        std::unique_lock<std::mutex> lock(_queue_mutex);
        _cond.wait(lock, [this] { return !_message_queue.empty() || b_stop.load(); });
        if (b_stop.load()) {
            while (!_message_queue.empty()) {
                _message_queue.pop();
            }
            break;
        }
        if (!_message_queue.empty()) {
            LogicSystem_Task task = std::move(_message_queue.front());
            _message_queue.pop();

            auto& req = task.req;
            auto& session = task.session;
            if (req.method() == http::verb::get) {
                std::cout << "Processing GET request" << std::endl;
                handle_get_request(req, session);
            } else if (req.method() == http::verb::post) {
                std::cout << "Processing POST request" << std::endl;
                handle_post_request(req, session);
            }
        }
    }
}

void LogicSystem::handle_get_request(const http::request<http::string_body>& req, std::shared_ptr<Session> session) {
    std::string target = std::string(req.target());
    if (get_handlers.contains(target)) {
        get_handlers[target](session);
    } else {
        std::cerr << "Unknown GET target: " << target << std::endl;
        json rsp_json;
        rsp_json["error"] = "Unknown GET target: " + target;
        send_json_response(session, http::status::not_found, rsp_json, "application/json");
    }
}

void LogicSystem::handle_post_request(const http::request<http::string_body>& req, std::shared_ptr<Session> session) {
    try {
        json root = json::parse(req.body());
        std::string action = root.value("action", "");
        std::string requester_id = root.value("from", root.value("id", ""));
        if(action.empty()) {
            std::cerr << "Missing action in POST request" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing action";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        if(action == "refresh_token") {
            bool token_valid = checkRefreshToken(requester_id, root.value("refresh_token", ""));
            if (!token_valid) {
                json rsp_json;
                rsp_json["error"] = "Invalid or expired refresh_token";
                send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
                return;
            }
        } else if(action != "register_user" && action != "login" && action != "logout") {
            bool token_valid = checkAccessToken(requester_id, root.value("access_token", ""));
            if (!token_valid) {
                json rsp_json;
                rsp_json["error"] = "Invalid or expired access_token";
                send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
                return;
            }
        }
        if (post_handlers.contains(action)) {
            post_handlers[action](root, session);
        } else {
            std::cerr << "Unknown POST action: " << action << std::endl;
            json rsp_json;
            rsp_json["error"] = "Unknown POST action: " + action;
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
        }
    } catch (const std::exception& e) {
        std::cerr << "Invalid POST body: " << e.what() << std::endl;
        json rsp_json;
        rsp_json["error"] = "Invalid JSON body";
        send_json_response(session, http::status::bad_request, rsp_json, "application/json");
    }
}

void LogicSystem::register_get_handler() {
    get_handlers.emplace("example_get", [](std::shared_ptr<Session> session) {
        std::cout << "Handling example_get action" << std::endl;
        json rsp_json;
        rsp_json["message"] = "example_get ok";
        send_json_response(session, http::status::ok, rsp_json, "application/json");
    });
}

void LogicSystem::register_post_handler() {
    post_handlers.emplace("register_user", [](const json& data, std::shared_ptr<Session> session) {
        std::cout << "Handling register_user action" << std::endl;

        std::string id = data.value("from", data.value("id", ""));
        std::string pwd = data.value("pwd", "");
        if (id.empty() || pwd.empty()) {
            std::cerr << "Missing from/id or pwd in register_user action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing from (or id) or pwd";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        else if(MysqlManager::checkUserExists(id)){
            std::cerr << "User already exists: " << id << std::endl;
            json rsp_json;
            rsp_json["error"] = "User already exists";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        
        if(MysqlManager::addUser(id, pwd)){
            std::cout << "User registered successfully: " << id << std::endl;
            json rsp_json;
            rsp_json["message"] = "User registered successfully";
            send_json_response(session, http::status::ok, rsp_json, "application/json");
        }
        else{
            std::cerr << "Failed to register user: " << id << std::endl;
            json rsp_json;
            rsp_json["error"] = "Failed to register user";
            send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
        }
    });
    post_handlers.emplace("login", [](const json& data, std::shared_ptr<Session> session) {
        std::cout << "Handling login action" << std::endl;

        std::string id = data.value("from", data.value("id", ""));
        std::string pwd = data.value("pwd", "");
        if (id.empty() || pwd.empty()) {
            std::cerr << "Missing from/id or pwd in login action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing from (or id) or pwd";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        
        if(MysqlManager::checkUserExists(id)){
            if(MysqlManager::checkPwd(id, pwd)){
                std::cout << "User logged in successfully: " << id << std::endl;
                std::string access_token = LogicSystem::getInstance().generateToken();
                std::string refresh_token = LogicSystem::getInstance().generateToken();

                bool access_saved = RedisManager::getInstance().set(access_token_key(access_token), id, kAccessTokenExpireSeconds);
                bool refresh_saved = RedisManager::getInstance().set(refresh_token_key(refresh_token), id, kRefreshTokenExpireSeconds);
                if (!access_saved || !refresh_saved) {
                    json rsp_json;
                    rsp_json["error"] = "Failed to save login token";
                    send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
                    return;
                }

                json rsp_json;
                rsp_json["message"] = "User logged in successfully";
                rsp_json["token"] = access_token;
                rsp_json["access_token"] = access_token;
                rsp_json["access_expires_in"] = kAccessTokenExpireSeconds;
                rsp_json["refresh_token"] = refresh_token;
                rsp_json["refresh_expires_in"] = kRefreshTokenExpireSeconds;
                send_json_response(session, http::status::ok, rsp_json, "application/json");
            }
            else{
                std::cerr << "Invalid password for user: " << id << std::endl;
                json rsp_json;
                rsp_json["error"] = "Invalid password";
                send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
            }
        }
        else{
            std::cerr << "Invalid credentials for user: " << id << std::endl;
            json rsp_json;
            rsp_json["error"] = "Invalid credentials";
            send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
        }
    });

    post_handlers.emplace("refresh_token", [](const json& data, std::shared_ptr<Session> session) {
        std::string refresh_token = data.value("refresh_token", "");
        std::string requester_id = data.value("from", data.value("id", ""));
        if (refresh_token.empty()) {
            json rsp_json;
            rsp_json["error"] = "Missing refresh_token";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }

        std::string id;
        if (!RedisManager::getInstance().get(refresh_token_key(refresh_token), id) || id.empty()) {
            json rsp_json;
            rsp_json["error"] = "Invalid or expired refresh_token";
            send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
            return;
        }
        
        if(id != requester_id) {
            json rsp_json;
            rsp_json["error"] = "refresh_token does not match user id";
            send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
            return;
        }
        std::string new_access_token = LogicSystem::getInstance().generateToken();
        std::string new_refresh_token = LogicSystem::getInstance().generateToken();
        bool access_saved = RedisManager::getInstance().set(access_token_key(new_access_token), id, kAccessTokenExpireSeconds);
        bool refresh_saved = RedisManager::getInstance().set(refresh_token_key(new_refresh_token), id, kRefreshTokenExpireSeconds);

        if (!access_saved || !refresh_saved) {
            json rsp_json;
            rsp_json["error"] = "Failed to refresh token";
            send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
            return;
        }

        RedisManager::getInstance().del(refresh_token_key(refresh_token));

        json rsp_json;
        rsp_json["message"] = "Token refreshed successfully";
        rsp_json["token"] = new_access_token;
        rsp_json["access_token"] = new_access_token;
        rsp_json["access_expires_in"] = kAccessTokenExpireSeconds;
        rsp_json["refresh_token"] = new_refresh_token;
        rsp_json["refresh_expires_in"] = kRefreshTokenExpireSeconds;
        send_json_response(session, http::status::ok, rsp_json, "application/json");
    });

    post_handlers.emplace("logout", [](const json& data, std::shared_ptr<Session> session) {
        std::string from_id = data.value("from", data.value("id", ""));
        std::string access_token = data.value("access_token", "");
        std::string refresh_token = data.value("refresh_token", "");
        if (access_token.empty()) {
            json rsp_json;
            rsp_json["error"] = "Missing access_token";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        if (refresh_token.empty()) {
            json rsp_json;
            rsp_json["error"] = "Missing refresh_token";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        if(!RedisManager::getInstance().exists(access_token_key(access_token)) || !RedisManager::getInstance().exists(refresh_token_key(refresh_token))) {
            json rsp_json;
            rsp_json["error"] = "Invalid or expired tokens";
            send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
            return;
        }
        std::string redis_access_id, redis_refresh_id;
        RedisManager::getInstance().get(access_token_key(access_token), redis_access_id);
        RedisManager::getInstance().get(refresh_token_key(refresh_token), redis_refresh_id);
        if (redis_access_id.empty() || redis_refresh_id.empty() || redis_access_id != redis_refresh_id|| redis_access_id != from_id) {
            json rsp_json;
            rsp_json["error"] = "Invalid or expired tokens";
            send_json_response(session, http::status::unauthorized, rsp_json, "application/json");
            return;
        }

        RedisManager::getInstance().del(access_token_key(access_token));
        RedisManager::getInstance().del(refresh_token_key(refresh_token));

        json rsp_json;
        rsp_json["message"] = "Logged out successfully";
        send_json_response(session, http::status::ok, rsp_json, "application/json");
    });
}

bool LogicSystem::checkAccessToken(const std::string& id, const std::string& access_token) {
    if (id.empty() || access_token.empty()) {
        return false;
    }
    std::string redis_access_id;
    bool access_valid = RedisManager::getInstance().get(access_token_key(access_token), redis_access_id) && !redis_access_id.empty();
    return access_valid && redis_access_id == id;
}

bool LogicSystem::checkRefreshToken(const std::string& id, const std::string& refresh_token) {
    if (id.empty() || refresh_token.empty()) {
        return false;
    }
    std::string redis_refresh_id;
    bool refresh_valid = RedisManager::getInstance().get(refresh_token_key(refresh_token), redis_refresh_id) && !redis_refresh_id.empty();
    return refresh_valid && redis_refresh_id == id;
}

std::string LogicSystem::generateToken() {
    boost::uuids::uuid token = boost::uuids::random_generator()();
    return boost::uuids::to_string(token);
}

void LogicSystem::stop() {
    b_stop.store(true);
    _cond.notify_all();
}

LogicSystem::LogicSystem() {
    if (!MysqlManager::initPoolFromEnv()) {
        std::cerr << "Failed to initialize MySQL pool in LogicSystem constructor" << std::endl;
    }
    register_get_handler();
    register_post_handler();
    for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
        ThreadPool::getInstance().commit([this]() { processTasks(); });
    }
}