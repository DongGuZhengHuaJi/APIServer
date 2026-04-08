#include "logic_system.h"
#include "api_server.h"
#include "thread_pool.hpp"

#include <nlohmann/json.hpp>


namespace http = boost::beast::http;
using json = nlohmann::json;

namespace {
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

        std::string id = data.value("id", "");
        std::string pwd = data.value("pwd", "");
        if (id.empty() || pwd.empty()) {
            std::cerr << "Missing id or pwd in register_user action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing id or pwd";
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
}

void LogicSystem::stop() {
    b_stop.store(true);
    _cond.notify_all();
}

LogicSystem::LogicSystem() {
    register_get_handler();
    register_post_handler();
    for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
        ThreadPool::getInstance().commit([this]() { processTasks(); });
    }
}