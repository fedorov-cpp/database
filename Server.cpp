#include <boost/filesystem/path.hpp>
#include <boost/fusion/include/for_each.hpp>
#include <sstream>

#include "Server.h"

namespace database {
  Stats::Stats(boost::asio::io_service &io_service,
               boost::posix_time::seconds freq)
    : timer_(io_service, freq)
    , totalItemsInDb_(Database::getInstance().size())
  {
    timer_.async_wait([this](boost::system::error_code) { print(); });
  }

  void Stats::update(Operation operation,
                     Error error) {

    OperationStats &stats = operationStats_[operation];
    const bool failed = (error != Error::NONE);
    auto &cnt = (failed) ? stats.failed : stats.successful;
    cnt++;

    if ((operation == Operation::INSERT) && (!failed)) {
      totalItemsInDb_++;
    }
    if ((operation == Operation::DELETE) && (!failed)) {
      totalItemsInDb_--;
    }
  }

  void Stats::print() {
    std::stringstream ss;
    ss << "Total items currently in Database: " << totalItemsInDb_ << "\n";
    for (const auto &item: operationStats_) {
      ss << "Total "
         << operationToString(item.first)
         << " attempts since server's start"
         << " (successful/failed): "
         << item.second.successful << "/"
         << item.second.failed << "\n";
    }
    fprintf(stderr, "%s\n", ss.str().c_str());
    timer_.expires_at(timer_.expires_at() + STATS_PRINT_FREQ);
    timer_.async_wait([this](boost::system::error_code) { print(); });
  }

  Session::Session(boost::asio::ip::tcp::socket socket,
                   std::shared_ptr<Stats> stats)
      : socket_(std::move(socket))
      , bufSize_(0UL)
      , buf_()
      , request_()
      , response_()
      , stats_(std::move(stats))
  {}

  void Session::run() {
    read();
  }

  void Session::read() {
    auto self{shared_from_this()};
    socket_.async_read_some(
        boost::asio::buffer(&bufSize_, sizeof(size_t)),
        [this, self](boost::system::error_code ec, size_t) {
          if (!ec) {
            buf_.resize(bufSize_);
            socket_.async_read_some(
                boost::asio::buffer(buf_),
                [this, self](boost::system::error_code ec, size_t) {
                  if (!ec) {
                    Reader reader(boost::asio::const_buffer(buf_.data(), buf_.size()));
                    boost::fusion::for_each(request_, reader);
                    printRequest(request_);
                    handleRequest();
                    write();
                  } else {
                    fprintf(stderr, "Reading data error: %s\n", ec.message().c_str());
                  }
                }
            );
          } else {
            fprintf(stderr, "Reading size error: %s\n", ec.message().c_str());
          }
        });
  }

  void Session::write() {
    auto self{shared_from_this()};
    buf_.clear();
    Writer writer(buf_);
    boost::fusion::for_each(response_, writer);
    const size_t size = writer.buf().size();
    boost::asio::async_write(
        socket_,
        boost::asio::buffer(&size, sizeof(size_t)),
        [this, self](boost::system::error_code ec, size_t) {
          if (!ec) {
            boost::asio::async_write(
                socket_,
                boost::asio::buffer(buf_),
                [this, self](boost::system::error_code ec, size_t) {
                  if (!ec) {
                    printResponse(response_);
                    read();
                  } else {
                    fprintf(stderr, "Writing data error: %s\n", ec.message().c_str());
                  }
                }
            );
          } else {
            fprintf(stderr, "Writing size error: %s\n", ec.message().c_str());
          }
        }
    );
  }

  void Session::handleRequest() {
    using namespace database;

    auto &db = Database::getInstance();

    Error error;
    switch (request_.operation) {
      case Operation::INSERT: {
        error = db.ins(request_.key, request_.value);
        response_ = Response{request_.operation, error, Value()};
        break;
      }
      case Operation::UPDATE: {
        error = db.upd(request_.key, request_.value);
        response_ = Response{request_.operation, error, Value()};
        break;
      }
      case Operation::DELETE: {
        error = db.del(request_.key);
        response_ = Response{request_.operation, error, Value()};
        break;
      }
      case Operation::GET: {
        Value value;
        error = db.get(request_.key, value);
        response_ = Response{request_.operation, error, value};
        break;
      }
    }
    stats_->update(request_.operation, error);
  }

  const char *Server::DEFAULT_STORAGE_FILENAME = "storage.bin";

  Server::Server(boost::asio::ip::tcp::endpoint endpoint,
                 const boost::filesystem::path &storagePath)
      : endpoint_(std::move(endpoint))
      , acceptor_(network_io_service_, endpoint_)
      , socket_(network_io_service_)
      , stats_(nullptr) {
    Database::getInstance().init(storagePath.c_str());
    stats_ = std::make_shared<Stats>(timer_io_service_, STATS_PRINT_FREQ);
    accept();
  }

  void Server::run() {
    unsigned int n = std::thread::hardware_concurrency();
    std::vector<std::thread> pool((n > 2) ? n - 1 : 1);
    for (auto &thread : pool) {
      thread = std::thread{[this](){ network_io_service_.run(); }};
    }
    std::thread timerThread{[this](){ timer_io_service_.run(); }};
    for (auto &thread : pool) {
      thread.join();
    }
    timerThread.join();
  }

  void Server::accept() {
//    std::cout << "Running accept in thread: " << std::this_thread::get_id() << std::endl;
    acceptor_.async_accept(
        socket_,
        [this](boost::system::error_code ec) {
          if (!ec) {
            std::make_shared<Session>(std::move(socket_), stats_)->run();
          }
          accept();
        });
  }
} // namespace database::server