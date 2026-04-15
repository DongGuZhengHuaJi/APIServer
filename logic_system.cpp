#include "logic_system.h"
#include "api_server.h"
#include "thread_pool.hpp"
#include "redis_mgr.h"
#include <nlohmann/json.hpp>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>


namespace http = boost::beast::http;
using json = nlohmann::json;

namespace {
constexpr int kAccessTokenExpireSeconds = 15 * 60;
constexpr int kRefreshTokenExpireSeconds = 7 * 24 * 60 * 60;
constexpr const char* kReservationEventChannel = "meeting:reservation_events";
constexpr const char* kRoomClosedEventChannel = "meeting:room_closed_events";

std::string access_token_key(const std::string& token) {
    return "access:" + token;
}

std::string refresh_token_key(const std::string& token) {
    return "refresh:" + token;
}

bool parse_unix_seconds(const std::string& value, std::chrono::system_clock::time_point& out) {
    try {
        size_t idx = 0;
        const long long sec = std::stoll(value, &idx);
        if (idx != value.size()) {
            return false;
        }

        if (sec < static_cast<long long>(std::numeric_limits<std::time_t>::min()) ||
            sec > static_cast<long long>(std::numeric_limits<std::time_t>::max())) {
            return false;
        }

        const std::time_t tt = static_cast<std::time_t>(sec);
        out = std::chrono::system_clock::from_time_t(tt);
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_datetime_local(const std::string& value, std::chrono::system_clock::time_point& out) {
    std::tm tm_buf{};
    {
        std::istringstream iss(value);
        iss >> std::get_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
        if (!iss.fail()) {
            const std::time_t tt = std::mktime(&tm_buf);
            if (tt == static_cast<std::time_t>(-1)) {
                return false;
            }
            out = std::chrono::system_clock::from_time_t(tt);
            return true;
        }
    }

    tm_buf = {};
    std::istringstream iss(value);
    iss >> std::get_time(&tm_buf, "%Y-%m-%d %H:%M");
    if (iss.fail()) {
        return false;
    }

    const std::time_t tt = std::mktime(&tm_buf);
    if (tt == static_cast<std::time_t>(-1)) {
        return false;
    }
    out = std::chrono::system_clock::from_time_t(tt);
    return true;
}

bool parse_time_value(std::string value, std::chrono::system_clock::time_point& out) {
    try {
        size_t idx = 0;
        long long raw = std::stoll(value, &idx);
        if (idx == value.size()) {
            if (raw >= 1000000000000LL || raw <= -1000000000000LL) {
                raw /= 1000;
            }

            if (raw >= static_cast<long long>(std::numeric_limits<std::time_t>::min()) &&
                raw <= static_cast<long long>(std::numeric_limits<std::time_t>::max())) {
                const std::time_t tt = static_cast<std::time_t>(raw);
                out = std::chrono::system_clock::from_time_t(tt);
                return true;
            }
        }
    } catch (...) {
        // Fall through to datetime string parsing.
    }

    if (parse_unix_seconds(value, out)) {
        return true;
    }

    if (!value.empty() && value.back() == 'Z') {
        value.pop_back();
    }
    for (char& c : value) {
        if (c == 'T') {
            c = ' ';
        }
    }
    const size_t dot_pos = value.find('.');
    if (dot_pos != std::string::npos) {
        value = value.substr(0, dot_pos);
    }

    return parse_datetime_local(value, out);
}

void send_json_response(std::shared_ptr<Session> session, http::status status, const json& rsp_json, const char* content_type) {
    http::response<http::string_body> rsp{status, 11};
    rsp.set(http::field::content_type, content_type);
    rsp.body() = rsp_json.dump();
    rsp.prepare_payload();
    session->send_response(rsp);
}

void delete_reservation_keys_by_room(RedisManager& redis_mgr, const std::string& room_id) {
    const std::string pattern = "reservation:*:" + room_id + ":*";
    long long cursor = 0;
    do {
        std::vector<std::string> keys;
        try {
            cursor = redis_mgr.getClient().scan(cursor, pattern, 200, std::back_inserter(keys));
        } catch (const std::exception& e) {
            std::cerr << "Redis scan failed for pattern " << pattern << ": " << e.what() << std::endl;
            return;
        }

        for (const auto& key : keys) {
            redis_mgr.del(key);
        }
    } while (cursor != 0);

    if (cursor == 0) {
        std::cout << "Reservation cache keys cleaned for room " << room_id << std::endl;
    }
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
    auto get_it = get_handlers.find(target);
    if (get_it != get_handlers.end()) {
        get_it->second(session);
    } else {
        std::cerr << "Unknown GET target: " << target << std::endl;
        json rsp_json;
        rsp_json["error"] = "Unknown GET target: " + target;
        send_json_response(session, http::status::not_found, rsp_json, "application/json");
    }
}

void LogicSystem::handle_post_request(const http::request<http::string_body>& req, std::shared_ptr<Session> session) {
    try {
        std::cout << "POST body bytes: " << req.body().size() << std::endl;
        json root = json::parse(req.body());
        std::string action = root.value("action", "");
        std::cout << "POST action: " << action << std::endl;
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
        auto post_it = post_handlers.find(action);
        if (post_it != post_handlers.end()) {
            post_it->second(root, session);
        } else {
            std::cerr << "Unknown POST action: " << action << std::endl;
            json rsp_json;
            rsp_json["error"] = "Unknown POST action: " + action;
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
        }
    } catch (const std::exception& e) {
        std::cerr << "Invalid POST body: " << e.what() << std::endl;
        std::cerr << "Raw POST body: " << req.body() << std::endl;
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
        std::string self_name = data.value("self_name", id);
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
        
        if(MysqlManager::addUser(id, pwd, self_name)){
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
                const std::string self_name = MysqlManager::getUserName(id);

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
                rsp_json["self_name"] = self_name;
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

    post_handlers.emplace("get_user_meetings",[](const json& data, std::shared_ptr<Session> session) {
        std::string user_id = data.value("from", data.value("id", ""));
            if (user_id.empty()) {
                send_json_response(session, http::status::bad_request, 
                    json{{"error", "Missing user id"}}, "application/json");
                return;
            }
            
            json meetings;
            if(MysqlManager::getUserMeetings(user_id, meetings)){
                std::cout << "Fetched meetings for user " << user_id << ": " << meetings.dump() << std::endl;
                json rsp_json;
                rsp_json["meetings"] = meetings;
                send_json_response(session, http::status::ok, rsp_json, "application/json");
            }
            else{
                std::cerr << "Failed to fetch meetings for user: " << user_id << std::endl;
                json rsp_json;
                rsp_json["error"] = "Failed to fetch meetings";
                send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
            }
        }
    );

    post_handlers.emplace("reserve", [](const json& data, std::shared_ptr<Session> session) {
        std::cout << "Handling reserve action" << std::endl;

        std::string id = data.value("from", data.value("id", ""));
        std::string time = data.value("time", "");
        std::string room = data.value("room", "");
        if (id.empty()) {
            std::cerr << "Missing from/id in reserve action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing from (or id)";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        else if (time.empty()) {
            std::cerr << "Missing time in reserve action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing time";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        else if (room.empty()) {
            std::cerr << "Missing room in reserve action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Missing room";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }
        
        std::chrono::system_clock::time_point reserve_time;
        if (!parse_time_value(time, reserve_time)) {
            std::cerr << "Invalid time format in reserve action" << std::endl;
            json rsp_json;
            rsp_json["error"] = "Invalid time, expected unix seconds/ms or YYYY-MM-DD HH:MM[:SS]";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }

        ReserveMeetingInfo info(id, reserve_time, room);
        bool mysql_written = false;
        bool redis_written = false;
        std::string reserve_key;
        
        try {
            if (!MysqlManager::addReservation(info)) {
                json rsp_json;
                rsp_json["error"] = "Failed to add reservation";
                send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
                return;
            }
            mysql_written = true;

            const long long reserve_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                reserve_time.time_since_epoch()).count();

            json reserve_event = {
                {"type", "reservation_created"},
                {"meeting_type", "reserved"},
                {"user_id", id},
                {"room", room},
                {"time", reserve_epoch}
            };

            RedisManager& redis_mgr = RedisManager::getInstance();
            reserve_key = "reservation:" + id + ":" + room + ":" + std::to_string(reserve_epoch);
            redis_written = redis_mgr.set(reserve_key, reserve_event.dump());
            if (!redis_written) {
                throw std::runtime_error("Failed to save reservation event to Redis");
            }

            // publish 返回订阅者数量。<=0 代表当前没有消费者，会导致预约事件丢失。
            const long long subscriber_count = redis_mgr.getClient().publish(kReservationEventChannel, reserve_event.dump());
            if (subscriber_count <= 0) {
                throw std::runtime_error("No signaling subscriber for reservation event");
            }
        } catch (const std::exception& e) {
            std::cerr << "Error adding reservation: " << e.what() << std::endl;

            if (redis_written && !reserve_key.empty()) {
                RedisManager::getInstance().del(reserve_key);
            }
            if (mysql_written) {
                const bool rollback_ok = MysqlManager::removeReservation(info);
                if (!rollback_ok) {
                    std::cerr << "Reservation rollback failed, manual cleanup may be required" << std::endl;
                }
            }

            json rsp_json;
            rsp_json["error"] = "Failed to add reservation";
            rsp_json["detail"] = e.what();
            send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
            return;
        }

        json rsp_json;
        rsp_json["status"] = "ok";
        send_json_response(session, http::status::ok, rsp_json, "application/json");
    });

    post_handlers.emplace("quick_meeting_start", [](const json& data, std::shared_ptr<Session> session) {
        std::string id = data.value("from", data.value("id", ""));
        std::string room = data.value("room", "");
        if (id.empty() || room.empty()) {
            json rsp_json;
            rsp_json["error"] = "Missing from/id or room";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }

        const auto now = std::chrono::system_clock::now();
        if (!MysqlManager::addQuickMeeting(id, room, now)) {
            json rsp_json;
            rsp_json["error"] = "Failed to persist quick meeting";
            send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
            return;
        }

        json rsp_json;
        rsp_json["status"] = "ok";
        send_json_response(session, http::status::ok, rsp_json, "application/json");
    });

    post_handlers.emplace("start_screen_share", [](const json& data, std::shared_ptr<Session> session) {
        std::string id = data.value("from", data.value("id", ""));
        std::string room = data.value("room", "");
        if (id.empty() || room.empty()) {
            json rsp_json;
            rsp_json["error"] = "Missing from/id or room";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }

        const auto now = std::chrono::system_clock::now();
        if (!MysqlManager::addScreenShareMeeting(id, room, now)) {
            json rsp_json;
            rsp_json["error"] = "Failed to persist screen share session";
            send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
            return;
        }

        json rsp_json;
        rsp_json["status"] = "ok";
        send_json_response(session, http::status::ok, rsp_json, "application/json");
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
        rsp_json["status"] = "ok";
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
        rsp_json["status"] = "ok";
        send_json_response(session, http::status::ok, rsp_json, "application/json");
    });

    post_handlers.emplace("update_user_name", [](const json& data, std::shared_ptr<Session> session) {
        const std::string id = data.value("from", data.value("id", ""));
        std::string self_name = data.value("self_name", "");

        if (id.empty()) {
            json rsp_json;
            rsp_json["error"] = "Missing from (or id)";
            send_json_response(session, http::status::bad_request, rsp_json, "application/json");
            return;
        }

        if (self_name.empty()) {
            self_name = id;
        }

        if (!MysqlManager::updateUserName(id, self_name)) {
            json rsp_json;
            rsp_json["error"] = "Failed to update self_name";
            send_json_response(session, http::status::internal_server_error, rsp_json, "application/json");
            return;
        }

        json rsp_json;
        rsp_json["status"] = "ok";
        rsp_json["self_name"] = MysqlManager::getUserName(id);
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

void LogicSystem::startRoomClosedSubscription() {
    bool expected = false;
    if (!_room_event_sub_started.compare_exchange_strong(expected, true)) {
        return;
    }

    std::thread([]() {
        try {
            auto subscriber = RedisManager::getInstance().getClient().subscriber();
            subscriber.on_message([](std::string channel, std::string msg) {
                if (channel != kRoomClosedEventChannel) {
                    return;
                }

                try {
                    const auto payload = nlohmann::json::parse(msg);
                    const std::string room_id = payload.value("room_id", "");
                    if (room_id.empty()) {
                        std::cerr << "Room close event missing room_id" << std::endl;
                        return;
                    }

                    const std::string reason = payload.value("reason", "empty_timeout");
                    const std::string meeting_type = payload.value("meeting_type", "reserved");
                    const auto closed_time = std::chrono::system_clock::now();

                    if (!MysqlManager::closeMeeting(room_id, reason, closed_time)) {
                        std::cerr << "Failed to close meeting records for room " << room_id
                                  << " type=" << meeting_type << std::endl;
                    }

                    RedisManager& redis_mgr = RedisManager::getInstance();
                    delete_reservation_keys_by_room(redis_mgr, room_id);
                    redis_mgr.set("room_status:" + room_id, "closed", 24 * 60 * 60);

                    std::cout << "Handled room closed event for room " << room_id << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Room close event parse/handle error: " << e.what() << std::endl;
                }
            });

            subscriber.subscribe(kRoomClosedEventChannel);
            while (true) {
                subscriber.consume();
            }
        } catch (const std::exception& e) {
            std::cerr << "Room closed subscription stopped: " << e.what() << std::endl;
        }
    }).detach();
}

LogicSystem::LogicSystem() {
    if (!MysqlManager::initPoolFromEnv()) {
        std::cerr << "Failed to initialize MySQL pool in LogicSystem constructor" << std::endl;
    }
    register_get_handler();
    register_post_handler();
    startRoomClosedSubscription();
    for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
        ThreadPool::getInstance().commit([this]() { processTasks(); });
    }
}