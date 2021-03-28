#include <iostream>
#include <tuple>

#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include <Server.h>

namespace { // anonymous

  const char *FS_PROGRAM_DESCRIPTION = R"(
Database Server listens requests on predefined IP:PORT.

Internal storage mapped to a file (path can be defined explicitly, otherwise
%s
path is used).
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

  const char *FS_ADDRESS_FORMAT = "<ip address>:<port>";


  class FS_ProgramOptionsParser {
    public:
      FS_ProgramOptionsParser()
        : _options("Command line options")
      {
        using namespace boost::program_options;

        _options.add_options()
            ("help,h", "Show help")
            ("address,a", value<std::string>()->required(), FS_ADDRESS_FORMAT)
            ("storage,s", value<std::string>(), "path to a storage-file");
      }

      bool parse(int argc, const char *argv[]) {
        bool ret = true;
        const boost::filesystem::path cmdPath{argv[0]};
        _defaultStoragePath = (boost::format("%s/%s")
                               % cmdPath.parent_path().string()
                               % database::Server::DEFAULT_STORAGE_FILENAME).str();

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
                      % _defaultStoragePath
                      % database::MAX_KEY_LENGTH
                      % database::MAX_VALUE_LENGTH).str()
                  << '\n';
        std::cout << _options << '\n';
      }

      auto getServerArgs() {
        using namespace boost::filesystem;

        notify(_vm);
        // notify will tell us if required options aren't set

        std::vector<std::string> tokens;
        const auto addr = _vm["address"].as<std::string>();
        boost::split(tokens, addr, boost::is_any_of(":"));
        if (2UL != tokens.size()) {
          throw std::invalid_argument{"unrecognized --address: " + addr +
                                      ", expected format: " +
                                      FS_ADDRESS_FORMAT};
        }
        const auto ip = boost::asio::ip::address::from_string(tokens[0]);
        const auto port = boost::lexical_cast<unsigned short>(tokens[1]);

        const auto storagePath = _vm.count("storage")
                                 ? path{_vm["storage"].as<std::string>()}
                                 : path{_defaultStoragePath};

        return std::make_tuple(ip, port, storagePath);
      }

    private:
      boost::program_options::options_description _options;
      boost::program_options::variables_map _vm;
      std::string _defaultStoragePath;
  };
} // namespace anonymous


int main(int argc, const char *argv[]) {
  FS_ProgramOptionsParser parser;

  if (parser.parse(argc, argv)) {
    if (parser.isHelpRequested()) {
      parser.showHelp();
    } else {
      const auto[ip, port, storagePath] = parser.getServerArgs();
      boost::asio::ip::tcp::endpoint endpoint{ip, port};
      database::Server server{endpoint, storagePath};
      server.run();
    }
  }

  return 0;
}