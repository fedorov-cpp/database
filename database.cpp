#include <unordered_map>
#include <iostream>

#include "database.h"

#if !defined(NDEBUG)
#define BOOST_MULTI_INDEX_ENABLE_INVARIANT_CHECKING
#define BOOST_MULTI_INDEX_ENABLE_SAFE_MODE
#endif

namespace database {
  std::string errorToString(Error e) {
    static const std::unordered_map<Error, std::string> ERRORS_MAP {
        {Error::NONE, "NONE"},
        {Error::DELETE_KEY_NOT_FOUND, "DELETE KEY NOT FOUND"},
        {Error::GET_KEY_NOT_FOUND, "GET KEY NOT FOUND"},
        {Error::INSERT_KEY_ALREADY_EXISTS, "INSERT KEY ALREADY EXISTS"},
        {Error::INVALID_KEY_LENGTH, "INVALID KEY LENGTH"},
        {Error::INVALID_VALUE_LENGTH, "INVALID VALUE LENGTH"},
        {Error::UPDATE_KEY_NOT_FOUND, "UPDATE KEY NOT FOUND"},
        {Error::UPDATE_VALUE_ALREADY_EXISTS, "UPDATE VALUE ALREADY EXISTS"}
    };

    const auto it = ERRORS_MAP.find(e);
    if (it != ERRORS_MAP.end()) {
      return it->second;
    }

    throw std::invalid_argument{"got unknown database::Error"};
  }

  std::string operationToString(Operation o) {
    static const std::unordered_map<Operation, std::string> OPERATIONS_MAP {
        {Operation::INSERT, "INSERT"},
        {Operation::UPDATE, "UPDATE"},
        {Operation::DELETE, "DELETE"},
        {Operation::GET, "GET"}
    };

    const auto it = OPERATIONS_MAP.find(o);
    if (it != OPERATIONS_MAP.end()) {
      return it->second;
    }

    throw std::invalid_argument{"got unknown database::Operation"};
  }

  Reader::Reader(boost::asio::const_buffer buf)
    : buf_(buf)
  {}

  Writer::Writer(std::vector<char> &buf)
    : buf_(buf)
  {}

  void printResponse(const Response &response) {
    std::stringstream ss;
    ss << "Response {";
    ss << "Operation: " << operationToString(response.operation);
    switch (response.operation) {
      case Operation::INSERT:
      case Operation::UPDATE:
      case Operation::DELETE: {
        if (Error::NONE == response.error) {
          ss << ", SUCCESS";
        } else {
          ss << ", ERROR: " << errorToString(response.error);
        }
        break;
      }
      case Operation::GET: {
        if (Error::NONE == response.error) {
          ss << ", SUCCESS, got value: " << response.value;
        } else {
          ss << ", ERROR: " << errorToString(response.error);
        }
        break;
      }
    }
    ss << "}";
    printf("%s\n", ss.str().c_str());
  }

  void printRequest(const Request &request) {
    std::stringstream ss;
    ss << "Request {";
    ss << "Operation: " << operationToString(request.operation);
    switch (request.operation) {
      case Operation::INSERT:
      case Operation::UPDATE: {
        ss << ", Key: " << request.key;
        ss << ", Value: " << request.value;
        break;
      }
      case Operation::DELETE:
      case Operation::GET: {
        ss << ", Key: " << request.key;
        break;
      }
    }
    ss << "}";
    printf("%s\n", ss.str().c_str());
  }

  const char *Database::DB_MTX_NAME = "DATABASE_MUTEX";

  Database::Database()
    : db_(nullptr)
    , db_mtx_(boost::interprocess::open_or_create, DB_MTX_NAME)
  {}

  Database& Database::getInstance() {
    static Database database;
    return database;
  }

  const char *Database::DB_NAME = "DATABASE_NAME";

  void Database::init(const char *filename) {

    const auto size = static_cast<unsigned long>(sysconf(_SC_PAGE_SIZE));

    seg_ = boost::interprocess::managed_mapped_file(
        boost::interprocess::open_or_create, filename, size);

    db_ = seg_.find_or_construct<Db>(DB_NAME)
        (Db::ctor_args_list(), Db::allocator_type(seg_.get_segment_manager()));
  }

  Error Database::ins(const Key &rawKey, const Value &rawValue) {
    using namespace boost::interprocess;

    scoped_lock<named_mutex> lock(db_mtx_);

    String key(rawKey.c_str(), String::allocator_type(seg_.get_segment_manager()));
    String value(rawValue.c_str(), String::allocator_type(seg_.get_segment_manager()));

    auto &index = db_->get<0>();
    auto it = index.find(key);
    if (it != index.end()) { return Error::INSERT_KEY_ALREADY_EXISTS; }

    auto result = db_->emplace(Item{key, value});
    return (result.second) ? Error::NONE : Error::INSERT_KEY_ALREADY_EXISTS;
  }

  Error Database::upd(const Key &rawKey, const Value &rawValue) {
    using namespace boost::interprocess;

    scoped_lock<named_mutex> lock(db_mtx_);

    String key(rawKey.c_str(), String::allocator_type(seg_.get_segment_manager()));
    String value(rawValue.c_str(), String::allocator_type(seg_.get_segment_manager()));

    auto &index = db_->get<0>();
    auto it = index.find(key);
    if (it == index.end()) { return Error::UPDATE_KEY_NOT_FOUND; }
    if (it->value == value) { return Error::UPDATE_VALUE_ALREADY_EXISTS; }

    index.modify(it, [&value](Item &i) { i.value = value; });
    return Error::NONE;
  }

  Error Database::del(const Key &rawKey) {
    using namespace boost::interprocess;

    scoped_lock<named_mutex> lock(db_mtx_);

    String key(rawKey.c_str(), String::allocator_type(seg_.get_segment_manager()));

    auto &index = db_->get<0>();
    auto it = index.find(key);
    if (it == index.end()) { return Error::DELETE_KEY_NOT_FOUND; }

    index.erase(it);
    return Error::NONE;
  }

  Error Database::get(const Key &rawKey, Value &value) {
    using namespace boost::interprocess;

    scoped_lock<named_mutex> lock(db_mtx_);

    String key(rawKey.c_str(), String::allocator_type(seg_.get_segment_manager()));

    auto &index = db_->get<0>();
    auto it = index.find(key);
    if (it == index.end()) { return Error::GET_KEY_NOT_FOUND; }

    value = it->value;
    return Error::NONE;
  }

  size_t Database::size() {
    using namespace boost::interprocess;

    size_t size = 0UL;

    {
      scoped_lock<named_mutex> lock(db_mtx_);
      auto &index = db_->get<0>();
      size = index.size();
    }
    return size;
  }
} // namespace database