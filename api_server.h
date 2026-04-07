#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <memory>

namespace beast = boost::beast;       
namespace http = boost::beast::http;
namespace net = boost::asio;

class APIServer; // 前向声明

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(net::io_context& ioc, net::ip::tcp::socket socket, std::shared_ptr<APIServer> server);
    void start();
    void send_response(const http::response<http::string_body>& rsp);

private:
    void do_read();

    net::io_context& _ioc;
    beast::tcp_stream _stream;
    beast::flat_buffer _buffer;
    std::shared_ptr<APIServer> _server;
    http::request<http::string_body> _req;
};

class APIServer : public std::enable_shared_from_this<APIServer> {
public:
    APIServer(net::io_context& ioc, short port);
    void start();
    void stop();

private:
    net::io_context& _ioc;
    net::ip::tcp::acceptor _acceptor;
};