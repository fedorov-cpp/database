#ifndef DATABASE_SERVER_H
#define DATABASE_SERVER_H

#include <memory>
#include <unordered_map>

#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <database.h>

namespace database {
  const boost::posix_time::seconds STATS_PRINT_FREQ {60};

  /**
   * Class keeps statistics about sent and received commands
   * and prints statistics to std::cerr once in defined period:
   * - number of items in database
   * - number of successful/failed INSERTs
   * - number of successful/failed UPDATEs
   * - number of successful/failed DELETEs
   * - number of successful/failed GETs
   */
  class Stats {
    public:
      Stats(boost::asio::io_service &io_service,
            boost::posix_time::seconds freq);

      void update(Operation, Error);

    private:
      void print();

      boost::asio::deadline_timer timer_;

      using Counter = std::atomic<size_t>;
      Counter totalItemsInDb_ {0UL};
      struct OperationStats {
        Counter successful {0UL};
        Counter failed {0UL};
      };
      std::unordered_map<Operation, OperationStats> operationStats_;
  };

  class Session: public std::enable_shared_from_this<Session> {
    public:
      Session(boost::asio::ip::tcp::socket,
              std::shared_ptr<Stats>);

      void run();

    private:
      void read();
      void write();
      void handleRequest();

      boost::asio::ip::tcp::socket socket_;
      size_t bufSize_;
      std::vector<char> buf_;
      Request request_;
      Response response_;
      std::shared_ptr<Stats> stats_;
  };

  /**
   * Database Server handles Clients requests to
   * get and set data. The data is stored in mapped file.
   */
  class Server {
    public:
      static const char *DEFAULT_STORAGE_FILENAME;

      Server(boost::asio::ip::tcp::endpoint endpoint,
             const boost::filesystem::path &storagePath);

      void run();

    private:
      void accept();

      boost::asio::io_service network_io_service_;
      boost::asio::ip::tcp::endpoint endpoint_;
      boost::asio::ip::tcp::acceptor acceptor_;
      boost::asio::ip::tcp::socket socket_;
      boost::asio::io_service timer_io_service_;
      std::shared_ptr<Stats> stats_;
  };
} // namespace database


#endif // DATABASE_SERVER_H
