#ifndef DATABASE_CLIENT_H
#define DATABASE_CLIENT_H

#include <tuple>

#include <boost/asio.hpp>
#include <boost/optional.hpp>

#include "database.h"

namespace database {
  /**
   * Database Client sends requests to Server in order
   * to get or set data.
   */
  class Client {
    public:
      explicit Client(boost::asio::io_service &io_service);
      /**
       * Function sends request to specified Server address and
       * returns Answer if operation was successful and
       * database::Error otherwise.
       */
      boost::system::error_code send(
          boost::asio::ip::tcp::endpoint endpoint,
          const Request &,
          Response &);

    private:
      boost::system::error_code write(const Request &request);
      boost::system::error_code read(Response &response);

      /**
       * Client remembers the last Server's endpoint
       * defined by user in order to provide convenience of
       * sending next request to the same Server
       */
      boost::asio::ip::tcp::endpoint lastEndpoint_;
      boost::asio::ip::tcp::socket socket_;
      bool m_isConnected;
  };
} // namespace database

#endif //DATABASE_CLIENT_H
