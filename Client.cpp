#include <boost/fusion/include/for_each.hpp>

#include "Client.h"

namespace database {
  Client::Client(boost::asio::io_service &io_service)
    : socket_(io_service)
    , m_isConnected(false)
  {}

  boost::system::error_code
  Client::send(boost::asio::ip::tcp::endpoint endpoint,
               const Request &request,
               Response &response) {
    if (!isValidKey(request.key)) {
      throw std::invalid_argument{"Got KEY exceeding max length of " + std::to_string(MAX_KEY_LENGTH) + " characters"};
    }

    if (!isValidValue(request.value)) {
      throw std::invalid_argument{"Got VALUE exceeding max length of " + std::to_string(MAX_VALUE_LENGTH) + " characters"};
    }

    if (endpoint != lastEndpoint_) {
      lastEndpoint_ = std::move(endpoint);
    }


    boost::system::error_code errorCode;
    if (!m_isConnected) {
      socket_.connect(lastEndpoint_, errorCode);
      if (!errorCode) {
        m_isConnected = true;
      }
    }
    if (m_isConnected) {
      errorCode = write(request);
      if (!errorCode) {
        errorCode = read(response);
      }
      if (errorCode) {
        socket_.close();
        m_isConnected = false;
      }
    }
    return errorCode;
  }

  boost::system::error_code Client::write(const Request &request) {
    std::vector<char> buf;
    Writer writer(buf);
    boost::fusion::for_each(request, writer);
    const size_t size = writer.buf().size();
    boost::system::error_code ec;
    boost::asio::write(socket_, boost::asio::buffer(&size, sizeof(size_t)), ec);
    if (!ec) {
      boost::asio::write(socket_, writer.buf(), ec);
    }
    return ec;
  }

  boost::system::error_code Client::read(Response &response) {
    size_t size = 0UL;
    boost::system::error_code ec;
    boost::asio::read(socket_, boost::asio::buffer(&size, sizeof(size_t)), ec);
    if (!ec) {
      std::vector<char> buf(size);
      boost::asio::read(socket_, boost::asio::buffer(buf), ec);
      Reader reader(
          boost::asio::const_buffer(buf.data(), buf.size()));
      boost::fusion::for_each(response, reader);
    }
    return ec;
  }
} // namespace database