#include <iostream>
#include <tuple>

#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/optional.hpp>

#include "Client.h"

namespace { // anonymous

  const char *FS_PROGRAM_DESCRIPTION = R"(
Database Client sends requests to Server using following information:
1) server ip address and port
2) operation
3) key
4) value

Internal database storage is a key-value storage with following constraints:
1) KEY is a string with max length equal to %ull characters
2) VALUE is a string with max length equal to %ull characters

Supported operations:
1) INSERT - insert key:value
2) UPDATE - update key:value
3) DELETE - delete key
4) GET    - get value using key

In the following error cases server will respond with error:
1) Attempt to INSERT a key which already exists
2) Attempt to UPDATE a key which doesn't exist
3) Attempt to UPDATE a key to the same value
4) Attempt to DELETE a key which doesn't exist
5) Attempt to GET a value from key which doesn't exist
)";

//  const char *FS_ADDRESS_FORMAT = "<ip address>:<port>";


  class FS_ProgramOptionsParser {
    public:
      FS_ProgramOptionsParser()
          : _options("Command line options") {
        using namespace boost::program_options;

        _options.add_options()
            ("help,h", "Show help");
      }

      bool parse(int argc, const char *argv[]) {
        bool ret = true;
        try {
          const auto parsed = parse_command_line(argc, argv, _options);
          store(parsed, _vm);
        } catch (const boost::program_options::error &ex) {
          std::cerr << ex.what() << '\n';
          ret = false;
        }

        return ret;
      }

      bool isHelpRequested() {
        return _vm.count("help");
      }

      void showHelp() {
        std::cout << (boost::format(FS_PROGRAM_DESCRIPTION)
                      % database::MAX_KEY_LENGTH
                      % database::MAX_VALUE_LENGTH).str()
                  << '\n';
        std::cout << _options << '\n';
      }

    private:
      boost::program_options::options_description _options;
      boost::program_options::variables_map _vm;
  };

  /**
   * Read address to connect
   */
  std::tuple<boost::asio::ip::address, unsigned short> FS_getAddress() {
    std::cout << "Enter server's ip address: ";
    std::string addressStr;
    std::getline(std::cin, addressStr);
    auto address = boost::asio::ip::address::from_string(addressStr);

    std::cout << "Enter server's port: ";
    std::string portStr;
    std::getline(std::cin, portStr);
    auto port = boost::lexical_cast<unsigned short>(portStr);

    return std::make_tuple(address, port);
  }

  /**
   * Read next user's input (request) from console
   */
  database::Request FS_getNextRequest() {

    std::cout << "Enter operation "
                 "["
                 "0|I|INSERT, "
                 "1|U|UPDATE, "
                 "2|D|DELETE, "
                 "3|G|GET"
                 "]: ";
    std::string operationStr;
    std::getline(std::cin, operationStr);

    const auto operation =
        boost::lexical_cast<database::Operation>(operationStr);

    std::cout << "Enter KEY: ";
    database::Key key;
    std::getline(std::cin, key);

    database::Value value;
    switch (operation) {
      case database::Operation::INSERT:
      case database::Operation::UPDATE:
        std::cout << "Enter VALUE: ";
        std::getline(std::cin, value);
        break;
      case database::Operation::GET:
      case database::Operation::DELETE:
        break;
    }

    return database::Request{operation, key, value};
  }
} // namespace anonymous


int main(int argc, const char *argv[]) {
  FS_ProgramOptionsParser parser;

  if (parser.parse(argc, argv)) {
    if (parser.isHelpRequested()) {
      parser.showHelp();
    } else {
      boost::asio::io_service io_service;
      database::Client client(io_service);
      boost::asio::ip::tcp::endpoint endpoint;
      while(true) {
        try {
          const auto[ip, port] = FS_getAddress();
          endpoint.address(ip);
          endpoint.port(port);
          break;
        } catch(std::exception &e) {
          fprintf(stderr, "%s\n", e.what());
        }
      }
      while(true) {
        try {
          const auto request = FS_getNextRequest();
          database::printRequest(request);
          database::Response response;
          const auto errorCode = client.send(endpoint, request, response);
          if (errorCode) {
            fprintf(stderr, "Error: %s\n", errorCode.message().c_str());
          } else {
            database::printResponse(response);
          }
        } catch(std::exception &e) {
          fprintf(stderr, "%s\n", e.what());
        }
      }
    }
  }

  return 0;
}