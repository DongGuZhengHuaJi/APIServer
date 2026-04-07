#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <iostream>
#include <thread>

#include "api_server.h"
#include "logic_system.h"
#include "thread_pool.hpp"

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace net = boost::asio;

APIServer::APIServer(boost::asio::io_context& ioc, short port)
    : _ioc(ioc), _acceptor(ioc, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)) {}

void APIServer::start() {
    _acceptor.async_accept(net::make_strand(_ioc), [self = shared_from_this()](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
        if (!ec) {
            std::cout << "New connection accepted" << std::endl;
            std::make_shared<Session>(self->_ioc, std::move(socket), self)->start();
        } else {
            std::cerr << "Accept error: " << ec.message() << std::endl;
        }
        if (self->_acceptor.is_open()) {
            self->start();
        }
    });
}

void APIServer::stop() {
    boost::system::error_code ec;
    _acceptor.close(ec);
    if (ec) {
        std::cerr << "Error closing acceptor: " << ec.message() << std::endl;
    }
    LogicSystem::getInstance().stop();
}

Session::Session(net::io_context& ioc, net::ip::tcp::socket socket, std::shared_ptr<APIServer> server)
    : _ioc(ioc), _stream(std::move(socket)), _server(server) {}

void Session::start() {
    do_read();
}

void Session::do_read() {
    _req = {};
    http::async_read(_stream, _buffer, _req, [self = shared_from_this()](beast::error_code ec, std::size_t /*bytes_transferred*/) {
        if (!ec) {
            std::cout << "Received request: " << self->_req.target() << std::endl;
            LogicSystem::getInstance().enqueueTask(LogicSystem_Task(std::move(self->_req), self));
        }
    });
}

void Session::send_response(const http::response<http::string_body>& rsp) {
    // 使用 post 确保代码切换回 I/O 线程执行，解决跨线程安全问题
    net::post(_stream.get_executor(), [self = shared_from_this(), rsp]() mutable {
        // 创建 shared_ptr 延长 response 的生命周期，直到异步写完成
        auto sp = std::make_shared<http::response<http::string_body>>(std::move(rsp));
        
        http::async_write(self->_stream, *sp, 
            [self, sp](beast::error_code ec, std::size_t) {
                if (!ec) {
                    beast::error_code ec_shutdown;
                    self->_stream.socket().shutdown(net::ip::tcp::socket::shutdown_send, ec_shutdown);
                }
            });
    });
}

int main() {
    try {
        boost::asio::io_context ioc;
        auto server = std::make_shared<APIServer>(ioc, 8888);

        std::promise<void> exit_signal;
        std::future<void> exit_future = exit_signal.get_future();
        server->start();
        for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
            ThreadPool::getInstance().commit([&ioc]() { ioc.run(); });
        }

        net::signal_set signals(net::make_strand(ioc), SIGINT, SIGTERM);
        signals.async_wait([&](const boost::system::error_code& error, int signal_number) {
            if (!error) {
                std::cout << "Signal received, shutting down..." << std::endl;
                server->stop();
                exit_signal.set_value();
            }
        });


        std::cout << "API Server running on port 8888..." << std::endl;
        std::cout << std::thread::hardware_concurrency() << " threads in the pool." << std::endl;
        std::cout << "Press Ctrl+C to stop the server." << std::endl;

        exit_future.wait(); // 等待退出信号

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}