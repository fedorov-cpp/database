#ifndef DATABASE_DATABASE_H
#define DATABASE_DATABASE_H

#include <cstddef>
#include <unordered_map>

#include <boost/lexical_cast.hpp>
#include <boost/fusion/include/define_struct.hpp>
#include <boost/fusion/tuple.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/sync/named_mutex.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/member.hpp>

namespace database {
  /**
    In the following error cases Server will respond with ServerError:
    1) Attempt to INSERT a key which already exists
    2) Attempt to UPDATE a key which doesn't exist
    3) Attempt to UPDATE a key to the same value
    4) Attempt to DELETE a key which doesn't exist
    5) Attempt to GET a value from key which doesn't exist
   */
  enum class Error : uint8_t {
      NONE,
      INSERT_KEY_ALREADY_EXISTS,
      UPDATE_KEY_NOT_FOUND,
      UPDATE_VALUE_ALREADY_EXISTS,
      DELETE_KEY_NOT_FOUND,
      GET_KEY_NOT_FOUND,
      INVALID_KEY_LENGTH,
      INVALID_VALUE_LENGTH
  };

  std::string errorToString(Error);

  /**
    Supported operations:
    1) INSERT - insert key:value
    2) UPDATE - update key:value
    3) DELETE - delete key
    4) GET    - get value using key
   */
  enum class Operation : uint8_t {
      INSERT,
      UPDATE,
      DELETE,
      GET
  };

  std::string operationToString(Operation);

  /**
    Internal storage is a key-value storage with following constraints:
    1) KEY is a string with max length equal to 1024 characters
    2) VALUE is a string with max length equal to (1024 * 1024) characters
   */

  const size_t MAX_KEY_LENGTH = 1024UL;
//  using Key = std::array<char, MAX_KEY_LENGTH>;
  using Key = std::string;

  inline bool isValidKey(const Key &key) {
    return (key.size() <= MAX_KEY_LENGTH);
  }

  const size_t MAX_VALUE_LENGTH = 1048576UL;
//  using Value = std::array<char, MAX_VALUE_LENGTH>;
  using Value = std::string;

  inline bool isValidValue(const Value &value) {
    return (value.size() <= MAX_VALUE_LENGTH);
  }

  template<typename T>
  T ntoh(T v) {
    constexpr size_t size = sizeof(T);
    if constexpr (16 == size) {
      return ntohs(v);
    } else if constexpr (32 == size) {
      return ntohl(v);
    } else if constexpr (64 == size) {
      return ntohll(v);
    } else {
      return v;
    }
  }

  template<typename T>
  T hton(T v) {
    constexpr size_t size = sizeof(T);
    if constexpr (16 == size) {
      return htons(v);
    } else if constexpr (32 == size) {
      return htonl(v);
    } else if constexpr (64 == size) {
      return htonll(v);
    } else {
      return v;
    }
  }

  class Reader {
    public:
      explicit Reader(boost::asio::const_buffer buf);

      template<class T>
      void operator()(T &val) const {
        val = ntoh(*boost::asio::buffer_cast<T const *>(buf_));
        buf_ = buf_ + sizeof(T);
      }

      template<>
      void operator()(std::string &val) const {
        uint16_t length = 0;
        (*this)(length);
        val = std::string(boost::asio::buffer_cast<char const*>(buf_), length);
        buf_ = buf_ + length;
      }

      inline boost::asio::const_buffer buf() const {
        return buf_;
      }

    private:
      mutable boost::asio::const_buffer buf_;
  };

  class Writer {
    public:
      explicit Writer(std::vector<char> &buf);

      template<class T>
      void operator()(T const &val) const {
        constexpr size_t valSize = sizeof(T);
        const size_t writeOffset = buf_.size();
        buf_.resize(buf_.size() + valSize);
        T tmp = hton(val);
        std::memcpy(buf_.data() + writeOffset, &tmp, valSize);
      }

      template<>
      void operator()(const std::string &val) const {
        const size_t valSize = val.length();
        (*this)(static_cast<uint16_t>(valSize));
        const size_t writeOffset = buf_.size();
        buf_.resize(buf_.size() + valSize);
        std::memcpy(buf_.data() + writeOffset, val.data(), valSize);
      }

      [[nodiscard]] inline boost::asio::const_buffer buf() const {
        return boost::asio::const_buffer(buf_.data(), buf_.size());
      }

    private:
      std::vector<char> &buf_;
  };

  class Database {
    public:
      Database(const Database &) = delete;

      Database &operator=(const Database &) = delete;

      static Database &getInstance();

      void init(const char *filename);

      Error ins(const Key &, const Value &);
      Error upd(const Key &, const Value &);
      Error del(const Key &);
      Error get(const Key &, Value &);
      size_t size();

    private:
      struct mutex_remove {
        mutex_remove() { boost::interprocess::named_mutex::remove(DB_MTX_NAME); }
        ~mutex_remove() { boost::interprocess::named_mutex::remove(DB_MTX_NAME); }
      };

      Database();

      using String = boost::interprocess::basic_string<
          char,
          std::char_traits<char>,
          boost::interprocess::allocator<
              char,
              boost::interprocess::managed_mapped_file::segment_manager
          >
      >;

      struct Item {
        String key;
        String value;
      };

      using Db = boost::multi_index_container<
          Item,
          boost::multi_index::indexed_by<
              boost::multi_index::hashed_unique<
                  BOOST_MULTI_INDEX_MEMBER(Item, String, key)
              >
          >,
          boost::interprocess::allocator<
              Item,
              boost::interprocess::managed_mapped_file::segment_manager
          >
      >;

      static const char *DB_NAME;
      Db *db_;
      static const char *DB_MTX_NAME;
      boost::interprocess::named_mutex db_mtx_;
      const mutex_remove mtx_remover_;
      boost::interprocess::managed_mapped_file seg_;
  };
} // namespace database

BOOST_FUSION_DEFINE_STRUCT(
    (database), Request,
    (database::Operation, operation)
        (database::Key, key)
        (database::Value, value)
)

BOOST_FUSION_DEFINE_STRUCT(
    (database), Response,
    (database::Operation, operation)
        (database::Error, error)
        (database::Value, value)
)

namespace database {
  void printResponse(const Response &);

  void printRequest(const Request &);
} // namespace database

namespace boost {
  // Allow lexical_cast from string to Operation
  template<>
  database::Operation lexical_cast(const std::string &s) {
    static const std::unordered_map<std::string, database::Operation> OPERATIONS_MAP {
        {"INSERT", database::Operation::INSERT},
        {"I", database::Operation::INSERT},
        {std::to_string(static_cast<uint8_t>(database::Operation::INSERT)), database::Operation::INSERT},
        {"UPDATE", database::Operation::UPDATE},
        {"U", database::Operation::UPDATE},
        {std::to_string(static_cast<uint8_t>(database::Operation::UPDATE)), database::Operation::UPDATE},
        {"DELETE", database::Operation::DELETE},
        {"D", database::Operation::DELETE},
        {std::to_string(static_cast<uint8_t>(database::Operation::DELETE)), database::Operation::DELETE},
        {"GET", database::Operation::GET},
        {"G", database::Operation::GET},
        {std::to_string(static_cast<uint8_t>(database::Operation::GET)), database::Operation::GET}
    };

    const auto it = OPERATIONS_MAP.find(s);
    if (it != OPERATIONS_MAP.end()) {
      return it->second;
    }

    boost::throw_exception(boost::bad_lexical_cast());
  }
} // namespace boost

#endif // DATABASE_DATABASE_H
