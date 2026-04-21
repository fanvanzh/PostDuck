#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/algorithm/string.hpp>

#include <unordered_map>
#include <mutex>
#include <atomic>
#include <random>
#include <cstring>
#include <unistd.h>

#include "session.hpp"
#include "log.hpp"
#include "db.hpp"

#include <memory>
#include <set>

using boost::asio::ip::tcp;
static boost::asio::thread_pool *thread_pool_ptr = nullptr;
static std::string datadir = ".";

// Global registry for backend pid -> session mapping (for CancelRequest handling)
static std::mutex sessions_mtx;
static std::unordered_map<uint32_t, std::weak_ptr<class PGSession>> sessions_map;
static std::unordered_map<uint32_t, uint32_t> sessions_secret;
static std::atomic<uint32_t> next_backend_pid{1};

// DuckDB LogicalTypeId -> PG type OID
// Based on duckdb::LogicalTypeId enum values.
static uint32_t pg_type_oid(const duckdb::LogicalType &lt)
{
    switch (lt.id())
    {
    case duckdb::LogicalTypeId::BOOLEAN:  return 16;   // bool
    case duckdb::LogicalTypeId::TINYINT:  return 21;   // int2 (no int1 in PG)
    case duckdb::LogicalTypeId::SMALLINT: return 21;   // int2
    case duckdb::LogicalTypeId::INTEGER:  return 23;   // int4
    case duckdb::LogicalTypeId::BIGINT:   return 20;   // int8
    case duckdb::LogicalTypeId::UTINYINT: return 21;
    case duckdb::LogicalTypeId::USMALLINT:return 23;
    case duckdb::LogicalTypeId::UINTEGER: return 20;
    case duckdb::LogicalTypeId::UBIGINT:  return 1700; // numeric (fit)
    case duckdb::LogicalTypeId::HUGEINT:  return 1700; // numeric
    case duckdb::LogicalTypeId::UHUGEINT: return 1700; // numeric
    case duckdb::LogicalTypeId::FLOAT:    return 700;  // float4
    case duckdb::LogicalTypeId::DOUBLE:   return 701;  // float8
    case duckdb::LogicalTypeId::DECIMAL:  return 1700; // numeric
    case duckdb::LogicalTypeId::VARCHAR:  return 25;   // text (better for JDBC generic)
    case duckdb::LogicalTypeId::CHAR:     return 1042; // bpchar
    case duckdb::LogicalTypeId::BLOB:     return 17;   // bytea
    case duckdb::LogicalTypeId::DATE:     return 1082;
    case duckdb::LogicalTypeId::TIME:     return 1083;
    case duckdb::LogicalTypeId::TIMESTAMP:     return 1114;
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:  return 1184;
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
    case duckdb::LogicalTypeId::TIMESTAMP_NS:  return 1114;
    case duckdb::LogicalTypeId::TIME_TZ:        return 1266;
    case duckdb::LogicalTypeId::INTERVAL:       return 1186;
    case duckdb::LogicalTypeId::UUID:           return 2950;
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::ARRAY:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::UNION:
    case duckdb::LogicalTypeId::ENUM:           return 25; // text repr
    default:                                    return 25;
    }
}

// Pick reasonable type length for a type OID; -1 = var length
static int16_t pg_type_len(uint32_t oid)
{
    switch (oid)
    {
    case 16:   return 1;   // bool
    case 17:   return -1;  // bytea
    case 18:   return 1;   // char
    case 20:   return 8;
    case 21:   return 2;
    case 23:   return 4;
    case 26:   return 4;   // oid
    case 700:  return 4;
    case 701:  return 8;
    case 1082: return 4;
    case 1083: return 8;
    case 1114: return 8;
    case 1184: return 8;
    case 1266: return 12;
    case 1186: return 16;
    case 2950: return 16;
    default:   return -1;
    }
}

void init_thread_pool(size_t thread_count)
{
    if (thread_pool_ptr != nullptr)
    {
        thread_pool_ptr->join();
        delete thread_pool_ptr;
    }
    thread_pool_ptr = new boost::asio::thread_pool(thread_count);
}

boost::asio::thread_pool &get_thread_pool()
{
    if (thread_pool_ptr == nullptr)
    {
        thread_pool_ptr = new boost::asio::thread_pool(4);
    }
    return *thread_pool_ptr;
}

void cleanup_thread_pool()
{
    if (thread_pool_ptr != nullptr)
    {
        thread_pool_ptr->join();
        delete thread_pool_ptr;
        thread_pool_ptr = nullptr;
    }
}

void set_data_directory(const std::string &dir)
{
    datadir = dir;
    if (!datadir.empty() && datadir.back() == '/')
    {
        datadir.pop_back();
    }
}

boost::asio::io_context &
PGSession::get_io_context()
{
    static boost::asio::io_context io_context_;
    return io_context_;
}

// --- parse startup params ---
void PGSession::parse_startup_params(const char *data, size_t length)
{
    size_t pos = 0;
    uint32_t version = ntohl(*reinterpret_cast<const uint32_t *>(data));
    startup_params_["version"] = std::to_string(version >> 16) + "." + std::to_string(version & 0xFFFF);
    pos += 4;
    while (pos < length)
    {
        std::string key(&data[pos]);
        pos += key.length() + 1;
        if (pos >= length)
            break;
        std::string value(&data[pos]);
        pos += value.length() + 1;
        startup_params_[key] = value;
    }
    std::string dump;
    for (const auto &param : startup_params_)
        dump += param.first + "=" + param.second + ", ";
    PDEBUG << "Startup: " << dump;
}

// --- SSL negotiation ---
void PGSession::handle_ssl_negotiation()
{
    auto buf = std::make_shared<std::array<char, 8>>();
    asio::async_read(socket_, asio::buffer(*buf, 8),
                     [self = shared_from_this(), buf](boost::system::error_code ec, size_t)
                     {
                         if (ec) return;
                         // SSLRequest code = 0x04D2162F (80877103)
                         if (memcmp(buf->data() + 4, "\x04\xD2\x16\x2F", 4) == 0)
                         {
                             asio::write(self->socket_, asio::buffer("N", 1));
                             // After rejecting SSL, client sends a fresh StartupMessage.
                             self->handle_ssl_negotiation();
                             return;
                         }
                         // GSSENCRequest code = 0x04D21630 (80877104)
                         if (memcmp(buf->data() + 4, "\x04\xD2\x16\x30", 4) == 0)
                         {
                             asio::write(self->socket_, asio::buffer("N", 1));
                             self->handle_ssl_negotiation();
                             return;
                         }
                         // CancelRequest code = 0x04D2162E
                         if (memcmp(buf->data() + 4, "\x04\xD2\x16\x2E", 4) == 0)
                         {
                             uint32_t pid = ntohl(*reinterpret_cast<uint32_t *>(buf->data() + 8 - 8)); // placeholder
                             (void)pid;
                             auto cbuf = std::make_shared<std::array<char, 8>>();
                             // Note: we've already consumed the 8 bytes (len + code).
                             // CancelRequest is exactly 16 bytes: len(4) code(4) pid(4) secret(4)
                             asio::async_read(self->socket_, asio::buffer(*cbuf, 8),
                                              [self, cbuf](boost::system::error_code ec2, size_t)
                                              {
                                                  if (ec2) return;
                                                  uint32_t pid = ntohl(*reinterpret_cast<uint32_t *>(cbuf->data()));
                                                  uint32_t secret = ntohl(*reinterpret_cast<uint32_t *>(cbuf->data() + 4));
                                                  std::shared_ptr<PGSession> target;
                                                  {
                                                      std::lock_guard<std::mutex> lg(sessions_mtx);
                                                      auto it = sessions_map.find(pid);
                                                      if (it != sessions_map.end() && !it->second.expired())
                                                      {
                                                          auto s_it = sessions_secret.find(pid);
                                                          if (s_it != sessions_secret.end() && s_it->second == secret)
                                                              target = it->second.lock();
                                                      }
                                                  }
                                                  if (target)
                                                  {
                                                      PINFO << "CancelRequest accepted pid=" << pid;
                                                      target->Cancel();
                                                  }
                                                  // cancel connection closes on return
                                              });
                             return;
                         }
                         // Regular startup packet: we already have first 8 bytes (len + version)
                         // store them into msg_buf_ to be re-used by handle_startup_continued
                         self->msg_buf_.assign(buf->begin(), buf->end());
                         // length includes itself
                         uint32_t total_len = ntohl(*reinterpret_cast<uint32_t *>(buf->data()));
                         if (total_len < 8 || total_len > 1024 * 1024)
                         {
                             PERROR << "Invalid startup packet length: " << total_len;
                             return;
                         }
                         size_t remaining = total_len - 8;
                         self->msg_buf_.resize(total_len);
                         if (remaining == 0)
                         {
                             self->parse_startup_params(self->msg_buf_.data() + 4, total_len - 4);
                             self->handle_authentication();
                             return;
                         }
                         asio::async_read(self->socket_,
                                          asio::buffer(self->msg_buf_.data() + 8, remaining),
                                          [self, total_len](boost::system::error_code ec3, size_t)
                                          {
                                              if (ec3) return;
                                              self->parse_startup_params(self->msg_buf_.data() + 4, total_len - 4);
                                              self->handle_authentication();
                                          });
                     });
}

// SSL negotiation already reads the first 8 bytes to check for SSL/Cancel/Startup.
// If it wasn't SSL/cancel, we treat those 8 bytes as the first bytes of the startup message.
// The old handle_startup() is replaced — keep a stub for ABI.
void PGSession::handle_startup() {}

void PGSession::handle_authentication()
{
    send_auth_ok();
}

// --- writing primitives ---
static inline void append_u8(std::vector<char> &buf, uint8_t v)
{
    buf.push_back(static_cast<char>(v));
}
static inline void append_u16(std::vector<char> &buf, uint16_t v)
{
    uint16_t n = htons(v);
    buf.insert(buf.end(), reinterpret_cast<char *>(&n), reinterpret_cast<char *>(&n) + 2);
}
static inline void append_i16(std::vector<char> &buf, int16_t v)
{
    uint16_t n = htons(static_cast<uint16_t>(v));
    buf.insert(buf.end(), reinterpret_cast<char *>(&n), reinterpret_cast<char *>(&n) + 2);
}
static inline void append_u32(std::vector<char> &buf, uint32_t v)
{
    uint32_t n = htonl(v);
    buf.insert(buf.end(), reinterpret_cast<char *>(&n), reinterpret_cast<char *>(&n) + 4);
}
static inline void append_i32(std::vector<char> &buf, int32_t v)
{
    uint32_t n = htonl(static_cast<uint32_t>(v));
    buf.insert(buf.end(), reinterpret_cast<char *>(&n), reinterpret_cast<char *>(&n) + 4);
}
static inline void append_cstr(std::vector<char> &buf, const std::string &s)
{
    buf.insert(buf.end(), s.begin(), s.end());
    buf.push_back('\0');
}

void PGSession::append_parameter_status(const std::string &name, const std::string &value)
{
    std::vector<char> msg;
    msg.push_back('S');
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0); // placeholder length
    append_cstr(msg, name);
    append_cstr(msg, value);
    uint32_t total = msg.size() - len_pos;
    uint32_t n = htonl(total);
    std::memcpy(msg.data() + len_pos, &n, 4);
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

void PGSession::send_auth_ok()
{
    // AuthenticationOk
    std::vector<char> auth_ok = {'R', 0, 0, 0, 8, 0, 0, 0, 0};
    out_buf_.insert(out_buf_.end(), auth_ok.begin(), auth_ok.end());

    // Parameter statuses required/expected by JDBC/psql
    append_parameter_status("server_version", "14.0 (PostDuck)");
    append_parameter_status("server_encoding", "UTF8");
    append_parameter_status("client_encoding", "UTF8");
    append_parameter_status("DateStyle", "ISO, MDY");
    append_parameter_status("TimeZone", "UTC");
    append_parameter_status("IntervalStyle", "postgres");
    append_parameter_status("integer_datetimes", "on");
    append_parameter_status("is_superuser", "on");
    append_parameter_status("session_authorization",
                            startup_params_.count("user") ? startup_params_["user"] : std::string("postduck"));
    append_parameter_status("standard_conforming_strings", "on");
    append_parameter_status("application_name",
                            startup_params_.count("application_name") ? startup_params_["application_name"] : std::string("postduck"));

    // BackendKeyData
    backend_pid_ = next_backend_pid.fetch_add(1);
    std::random_device rd;
    backend_secret_ = static_cast<uint32_t>(rd());
    {
        std::lock_guard<std::mutex> lg(sessions_mtx);
        sessions_map[backend_pid_] = shared_from_this();
        sessions_secret[backend_pid_] = backend_secret_;
    }
    std::vector<char> bk;
    bk.push_back('K');
    append_u32(bk, 12);
    append_u32(bk, backend_pid_);
    append_u32(bk, backend_secret_);
    out_buf_.insert(out_buf_.end(), bk.begin(), bk.end());

    // Attach + use the database named in startup. If "database" not provided, fallback to user or "postduck".
    std::string db_name;
    if (startup_params_.count("database"))
        db_name = startup_params_["database"];
    else if (startup_params_.count("user"))
        db_name = startup_params_["user"];
    else
        db_name = "postduck";

    try
    {
        if (!db_name.empty() && db_name != "memory" && db_name != ":memory:")
        {
            std::string attach_sql = "ATTACH IF NOT EXISTS '" + datadir + "/" + db_name + ".db' AS \"" + db_name + "\";";
            auto res = connection_->Query(attach_sql);
            if (res->HasError())
            {
                PDEBUG << "ATTACH failed, using in-memory: " << res->GetError();
            }
            else
            {
                auto use_res = connection_->Query("USE \"" + db_name + "\";");
                if (use_res->HasError())
                    PDEBUG << "USE failed: " << use_res->GetError();
                else
                    PDEBUG << "USE OK: " << db_name;
            }
        }
    }
    catch (std::exception &e)
    {
        PDEBUG << "attach exception: " << e.what();
    }

    enqueue_ready_for_query();
    flush_output();

    // start reading messages
    read_message();
}

// --- message reading loop ---
void PGSession::read_message()
{
    auto hdr = std::make_shared<std::array<char, 5>>();
    asio::async_read(socket_, asio::buffer(*hdr, 5),
                     [self = shared_from_this(), hdr](boost::system::error_code ec, size_t)
                     {
                         if (ec)
                         {
                             PDEBUG << "read_message end: " << ec.message();
                             return;
                         }
                         char type = (*hdr)[0];
                         uint32_t len = ntohl(*reinterpret_cast<uint32_t *>(hdr->data() + 1));
                         if (len < 4 || len > 1024 * 1024 * 64)
                         {
                             PERROR << "Invalid message length: " << len;
                             return;
                         }
                         auto body = std::make_shared<std::vector<char>>(len - 4);
                         if (body->empty())
                         {
                             self->dispatch_message(type, std::move(*body));
                             self->read_message();
                             return;
                         }
                         asio::async_read(self->socket_, asio::buffer(*body),
                                          [self, type, body](boost::system::error_code ec2, size_t)
                                          {
                                              if (ec2) return;
                                              self->dispatch_message(type, std::move(*body));
                                              self->read_message();
                                          });
                     });
}

void PGSession::dispatch_message(char msg_type, std::vector<char> &&body)
{
    // Extended protocol: when in error, skip until Sync
    if (in_error_ && msg_type != 'S' && msg_type != 'X')
    {
        return;
    }
    auto self = shared_from_this();
    auto body_shared = std::make_shared<std::vector<char>>(std::move(body));
    if (!strand_)
    {
        strand_ = std::make_shared<boost::asio::strand<boost::asio::thread_pool::executor_type>>(
            boost::asio::make_strand(get_thread_pool().get_executor()));
    }
    // Dispatch processing to thread pool via a per-session strand so messages for
    // the same session are processed in the order received (required by extended protocol).
    boost::asio::post(*strand_,
                      [self, msg_type, body_shared]()
                      {
                          try
                          {
                              switch (msg_type)
                              {
                              case 'Q':
                              {
                                  // simple query
                                  std::string q;
                                  if (!body_shared->empty())
                                      q.assign(body_shared->data(), body_shared->size() - 1); // strip trailing \0
                                  self->handle_simple_query(q);
                                  break;
                              }
                              case 'P': self->handle_parse(*body_shared); break;
                              case 'B': self->handle_bind(*body_shared); break;
                              case 'D': self->handle_describe(*body_shared); break;
                              case 'E': self->handle_execute(*body_shared); break;
                              case 'C': self->handle_close(*body_shared); break;
                              case 'H': self->handle_flush(); break;
                              case 'S': self->handle_sync(); break;
                              case 'X':
                                  // Terminate: do nothing, connection will close on read error
                                  break;
                              case 'd': // CopyData - unsupported
                              case 'c': // CopyDone
                              case 'f': // CopyFail
                                  self->enqueue_error("COPY protocol not supported", "0A000");
                                  self->enqueue_ready_for_query();
                                  self->flush_output();
                                  break;
                              default:
                                  PDEBUG << "Ignoring unknown message type: " << msg_type;
                                  break;
                              }
                          }
                          catch (std::exception &e)
                          {
                              PERROR << "dispatch exception: " << e.what();
                              self->enqueue_error(e.what(), "XX000");
                              self->in_error_ = true;
                              self->flush_output();
                          }
                      });
}

// Return true if the given trimmed, lower-cased statement is a transaction-control
// statement that we handle at the protocol layer.
static int txn_kind(const std::string &cmp_lower)
{
    // 1 = BEGIN, 2 = COMMIT, 3 = ROLLBACK, 0 = none
    if (boost::algorithm::starts_with(cmp_lower, "begin") ||
        boost::algorithm::starts_with(cmp_lower, "start transaction"))
        return 1;
    if (cmp_lower == "commit" || cmp_lower == "end" || cmp_lower == "commit work" ||
        cmp_lower == "commit transaction" || boost::algorithm::starts_with(cmp_lower, "commit "))
        return 2;
    if (cmp_lower == "rollback" || cmp_lower == "rollback work" || cmp_lower == "abort" ||
        cmp_lower == "rollback transaction" || boost::algorithm::starts_with(cmp_lower, "rollback "))
        return 3;
    return 0;
}

// --- simple query ---
void PGSession::handle_simple_query(const std::string &raw_query)
{
    std::string query = rewrite_query(raw_query);
    PDEBUG << "simple query: " << query;

    // Trim
    std::string trimmed = boost::algorithm::trim_copy(query);
    if (trimmed.empty())
    {
        enqueue_empty_query_response();
        enqueue_ready_for_query();
        flush_output();
        return;
    }

    // DuckDB supports multi-statement queries in a single call; we just pass through
    // but may need to split for correct per-statement CommandComplete handling.
    // Simpler: run the whole string and handle result. DuckDB returns the last result
    // linked via next_. We iterate.
    duckdb::unique_ptr<duckdb::QueryResult> result;
    try { result = connection_->SendQuery(query); }
    catch (std::exception &e)
    {
        enqueue_error(std::string("query failed: ") + e.what(), "XX000");
        enqueue_ready_for_query();
        flush_output();
        return;
    }
    // If a previous statement aborted the transaction, rollback and retry.
    if (result && result->HasError())
    {
        std::string elower = boost::algorithm::to_lower_copy(result->GetError());
        if (elower.find("current transaction is aborted") != std::string::npos)
        {
            try { connection_->Query("ROLLBACK;"); } catch (...) {}
            try { result = connection_->SendQuery(query); }
            catch (std::exception &e)
            {
                enqueue_error(std::string("query failed: ") + e.what(), "XX000");
                enqueue_ready_for_query();
                flush_output();
                return;
            }
        }
    }

    // Iterate through result chain (one entry per SQL statement)
    duckdb::QueryResult *cur = result.get();
    while (cur)
    {
        if (cur->HasError())
        {
            std::string err = cur->GetError();
            // Swallow benign transaction-state errors that arise from Postgres-style
            // redundant BEGIN/COMMIT: PG clients issue BEGIN when a txn is already
            // implicitly started and vice versa.
            std::string elower = boost::algorithm::to_lower_copy(err);
            if (elower.find("cannot start a transaction within a transaction") != std::string::npos ||
                elower.find("cannot commit - there is no transaction active") != std::string::npos ||
                elower.find("cannot rollback - there is no transaction active") != std::string::npos ||
                elower.find("no transaction is active") != std::string::npos)
            {
                std::string tag = "OK";
                if (elower.find("start") != std::string::npos) tag = "BEGIN";
                else if (elower.find("commit") != std::string::npos) tag = "COMMIT";
                else if (elower.find("rollback") != std::string::npos) tag = "ROLLBACK";
                enqueue_command_complete(tag);
                cur = cur->next.get();
                continue;
            }
            enqueue_error(err, "XX000");
            break;
        }

        duckdb::StatementType stmt_type = cur->statement_type;
        // Only SELECT/EXPLAIN (and EXECUTE of a prepared SELECT) stream rows to
        // the client. Everything else — DDL, DML, transaction control, SET,
        // PRAGMA — just emits a CommandComplete tag. DuckDB surfaces a 1-column
        // "Success"/"Count" result for many of these; we must not forward it as
        // a RowDescription or libpq will think the statement returned tuples.
        bool is_result_producing =
            (stmt_type == duckdb::StatementType::SELECT_STATEMENT) ||
            (stmt_type == duckdb::StatementType::EXPLAIN_STATEMENT) ||
            (stmt_type == duckdb::StatementType::EXECUTE_STATEMENT) ||
            (stmt_type == duckdb::StatementType::CALL_STATEMENT);
        bool is_dml = (stmt_type == duckdb::StatementType::INSERT_STATEMENT) ||
                      (stmt_type == duckdb::StatementType::UPDATE_STATEMENT) ||
                      (stmt_type == duckdb::StatementType::DELETE_STATEMENT) ||
                      (stmt_type == duckdb::StatementType::COPY_STATEMENT);
        bool is_select = is_result_producing;

        std::vector<ColumnDesc> cols;
        for (idx_t i = 0; i < cur->ColumnCount(); i++)
        {
            ColumnDesc c;
            c.name = cur->names[i];
            c.logical_type = cur->types[i];
            c.col_num = (uint16_t)(i + 1);
            cols.push_back(c);
        }

        idx_t row_count = 0;
        if (is_select)
        {
            enqueue_row_description(cols);
            std::vector<int16_t> fmts(cols.size(), 0);
            while (true)
            {
                auto chunk = cur->Fetch();
                if (!chunk || chunk->size() == 0) break;
                for (idx_t r = 0; r < chunk->size(); r++)
                    enqueue_data_row_chunk(*chunk, r, fmts);
                row_count += chunk->size();
            }
        }
        else
        {
            // Drain rows; DML statements typically return a single row with affected count.
            auto chunk = cur->Fetch();
            if (chunk && chunk->size() > 0 && chunk->ColumnCount() > 0)
            {
                try
                {
                    row_count = (idx_t)chunk->GetValue(0, 0).GetValue<int64_t>();
                }
                catch (...) { row_count = 0; }
            }
            // drain remaining (if any)
            while (chunk && chunk->size() > 0)
            {
                chunk = cur->Fetch();
            }
        }
        enqueue_command_complete(statement_tag_for(stmt_type, row_count));
        cur = cur->next.get();
    }

    enqueue_ready_for_query();
    flush_output();
}

// --- extended query: Parse ---
void PGSession::handle_parse(const std::vector<char> &body)
{
    // body: stmt_name \0 query \0 int16 nparams [oid...]
    size_t pos = 0;
    auto read_cstr = [&](std::string &out)
    {
        size_t start = pos;
        while (pos < body.size() && body[pos] != '\0') pos++;
        out.assign(body.data() + start, pos - start);
        if (pos < body.size()) pos++;
    };

    std::string stmt_name, query;
    read_cstr(stmt_name);
    read_cstr(query);
    if (pos + 2 > body.size()) { enqueue_error("malformed Parse", "08P01"); in_error_ = true; return; }
    uint16_t nparams = ntohs(*reinterpret_cast<const uint16_t *>(body.data() + pos));
    pos += 2;
    std::vector<uint32_t> param_oids;
    for (uint16_t i = 0; i < nparams; i++)
    {
        if (pos + 4 > body.size()) { enqueue_error("malformed Parse oid", "08P01"); in_error_ = true; return; }
        param_oids.push_back(ntohl(*reinterpret_cast<const uint32_t *>(body.data() + pos)));
        pos += 4;
    }

    std::string rewritten = rewrite_query(query);
    PDEBUG << "Parse '" << stmt_name << "' nparams=" << nparams << " oids=" << [&](){std::string s; for (auto o: param_oids) s += std::to_string(o) + ","; return s;}() << " sql=" << rewritten;

    auto entry = std::make_shared<PreparedStatementEntry>();
    entry->query = rewritten;
    entry->param_type_oids = param_oids;
    // Detect whether the query uses any $<n> parameters and whether the Parse message
    // supplied type OIDs for all of them. If any parameter's type is unknown we defer
    // preparation until Bind (at which point we inline the actual values).
    bool query_has_params = rewritten.find('$') != std::string::npos;
    bool has_untyped_params = false;
    if (query_has_params && param_oids.empty())
    {
        has_untyped_params = true;
    }
    for (auto oid : param_oids)
        if (oid == 0) { has_untyped_params = true; break; }
    if (!has_untyped_params)
    {
        try
        {
            entry->stmt = connection_->Prepare(rewritten);
            if (entry->stmt->HasError())
            {
                PDEBUG << "eager Prepare failed (defer to Bind): " << entry->stmt->GetError();
                entry->stmt.reset();
            }
        }
        catch (std::exception &e)
        {
            PDEBUG << "eager Prepare exception (defer to Bind): " << e.what();
            entry->stmt.reset();
        }
    }
    prep_map_[stmt_name] = entry;
    enqueue_parse_complete();
}

// Forward declarations
static std::string inline_parameters(const std::string &sql, const duckdb::vector<duckdb::Value> &values);

// Convert a PG wire parameter value to a duckdb::Value for prepared-statement binding.
static duckdb::Value pg_param_to_value(const char *data, int32_t len, int16_t format, uint32_t type_oid)
{
    if (len < 0)
    {
        return duckdb::Value(); // NULL
    }
    std::string s(data, data + len);
    if (format == 0)
    {
        // text format
        switch (type_oid)
        {
        case 16: // bool
            return duckdb::Value::BOOLEAN(s == "t" || s == "true" || s == "1" || s == "TRUE");
        case 20:
            try { return duckdb::Value::BIGINT(std::stoll(s)); } catch (...) { return duckdb::Value(s); }
        case 21:
            try { return duckdb::Value::SMALLINT((int16_t)std::stoi(s)); } catch (...) { return duckdb::Value(s); }
        case 23:
            try { return duckdb::Value::INTEGER(std::stoi(s)); } catch (...) { return duckdb::Value(s); }
        case 700:
            try { return duckdb::Value::FLOAT(std::stof(s)); } catch (...) { return duckdb::Value(s); }
        case 701:
            try { return duckdb::Value::DOUBLE(std::stod(s)); } catch (...) { return duckdb::Value(s); }
        case 0:
        {
            // Unknown type: try to infer from the text representation.
            // Integers, floats are common and get mis-parsed as strings otherwise.
            if (!s.empty())
            {
                size_t i = 0;
                bool neg = (s[0] == '-' || s[0] == '+');
                if (neg) i = 1;
                bool digits = (i < s.size());
                bool has_dot = false, has_exp = false;
                for (; i < s.size(); i++)
                {
                    char ch = s[i];
                    if (std::isdigit((unsigned char)ch)) continue;
                    if (ch == '.' && !has_dot && !has_exp) { has_dot = true; continue; }
                    if ((ch == 'e' || ch == 'E') && !has_exp)
                    {
                        has_exp = true;
                        if (i + 1 < s.size() && (s[i+1] == '+' || s[i+1] == '-')) i++;
                        continue;
                    }
                    digits = false;
                    break;
                }
                if (digits)
                {
                    try
                    {
                        if (!has_dot && !has_exp)
                        {
                            long long v = std::stoll(s);
                            if (v >= -2147483648LL && v <= 2147483647LL)
                                return duckdb::Value::INTEGER((int32_t)v);
                            return duckdb::Value::BIGINT((int64_t)v);
                        }
                        return duckdb::Value::DOUBLE(std::stod(s));
                    }
                    catch (...) {}
                }
            }
            return duckdb::Value(s);
        }
        default:
            return duckdb::Value(s);
        }
    }
    else
    {
        // binary format
        switch (type_oid)
        {
        case 16: // bool
            return duckdb::Value::BOOLEAN(len > 0 && data[0] != 0);
        case 21: // int2
            if (len == 2) return duckdb::Value::SMALLINT((int16_t)ntohs(*reinterpret_cast<const uint16_t *>(data)));
            break;
        case 23: // int4
            if (len == 4) return duckdb::Value::INTEGER((int32_t)ntohl(*reinterpret_cast<const uint32_t *>(data)));
            break;
        case 20: // int8
            if (len == 8)
            {
                uint64_t hi = ntohl(*reinterpret_cast<const uint32_t *>(data));
                uint64_t lo = ntohl(*reinterpret_cast<const uint32_t *>(data + 4));
                int64_t v = (int64_t)((hi << 32) | lo);
                return duckdb::Value::BIGINT(v);
            }
            break;
        case 700: // float4
            if (len == 4)
            {
                uint32_t i = ntohl(*reinterpret_cast<const uint32_t *>(data));
                float f;
                std::memcpy(&f, &i, 4);
                return duckdb::Value::FLOAT(f);
            }
            break;
        case 701: // float8
            if (len == 8)
            {
                uint64_t hi = ntohl(*reinterpret_cast<const uint32_t *>(data));
                uint64_t lo = ntohl(*reinterpret_cast<const uint32_t *>(data + 4));
                uint64_t i = (hi << 32) | lo;
                double d;
                std::memcpy(&d, &i, 8);
                return duckdb::Value::DOUBLE(d);
            }
            break;
        case 25:
        case 1043:
        case 1042:
            return duckdb::Value(s);
        default:
            break;
        }
        // Fall back to bytes as string
        return duckdb::Value(s);
    }
}

void PGSession::handle_bind(const std::vector<char> &body)
{
    size_t pos = 0;
    auto read_cstr = [&](std::string &out)
    {
        size_t start = pos;
        while (pos < body.size() && body[pos] != '\0') pos++;
        out.assign(body.data() + start, pos - start);
        if (pos < body.size()) pos++;
    };
    std::string portal_name, stmt_name;
    read_cstr(portal_name);
    read_cstr(stmt_name);

    if (pos + 2 > body.size()) { enqueue_error("malformed Bind", "08P01"); in_error_ = true; return; }
    uint16_t nfmts = ntohs(*reinterpret_cast<const uint16_t *>(body.data() + pos));
    pos += 2;
    std::vector<int16_t> param_formats(nfmts);
    for (uint16_t i = 0; i < nfmts; i++)
    {
        if (pos + 2 > body.size()) { enqueue_error("malformed Bind fmt", "08P01"); in_error_ = true; return; }
        param_formats[i] = (int16_t)ntohs(*reinterpret_cast<const uint16_t *>(body.data() + pos));
        pos += 2;
    }

    if (pos + 2 > body.size()) { enqueue_error("malformed Bind nparams", "08P01"); in_error_ = true; return; }
    uint16_t nparams = ntohs(*reinterpret_cast<const uint16_t *>(body.data() + pos));
    pos += 2;

    auto prep_it = prep_map_.find(stmt_name);
    if (prep_it == prep_map_.end())
    {
        enqueue_error("prepared statement \"" + stmt_name + "\" does not exist", "26000");
        in_error_ = true;
        return;
    }
    auto prep = prep_it->second;

    // Fetch expected DuckDB parameter types so we can coerce parameters correctly when
    // the client did not supply explicit type OIDs in Parse (as is typical with libpq extended protocol).
    duckdb::case_insensitive_map_t<duckdb::LogicalType> expected_types_map;
    if (prep->stmt)
    {
        try { expected_types_map = prep->stmt->GetExpectedParameterTypes(); }
        catch (...) {}
    }
    auto expected_type_for = [&](uint16_t idx) -> duckdb::LogicalType
    {
        std::string name = std::to_string(idx + 1);
        auto it = expected_types_map.find(name);
        if (it != expected_types_map.end()) return it->second;
        return duckdb::LogicalType(duckdb::LogicalTypeId::INVALID);
    };

    duckdb::vector<duckdb::Value> values;
    values.reserve(nparams);
    for (uint16_t i = 0; i < nparams; i++)
    {
        if (pos + 4 > body.size()) { enqueue_error("malformed Bind param len", "08P01"); in_error_ = true; return; }
        int32_t plen = (int32_t)ntohl(*reinterpret_cast<const uint32_t *>(body.data() + pos));
        pos += 4;
        int16_t fmt = 0;
        if (nfmts == 1) fmt = param_formats[0];
        else if (nfmts > 1 && i < nfmts) fmt = param_formats[i];
        uint32_t type_oid = (i < prep->param_type_oids.size()) ? prep->param_type_oids[i] : 0;

        const char *data_ptr = (plen < 0) ? nullptr : (body.data() + pos);
        duckdb::Value v = pg_param_to_value(data_ptr, plen, fmt, type_oid);
        // If the prepared statement expects a specific type, coerce.
        if (!v.IsNull())
        {
            auto expected = expected_type_for(i);
            if (expected.id() != duckdb::LogicalTypeId::INVALID &&
                expected.id() != duckdb::LogicalTypeId::UNKNOWN &&
                v.type() != expected)
            {
                duckdb::Value casted;
                std::string err;
                if (v.DefaultTryCastAs(expected, casted, &err))
                    v = casted;
            }
        }
        values.push_back(std::move(v));
        if (plen > 0) pos += plen;
    }

    if (pos + 2 > body.size()) { enqueue_error("malformed Bind rfmts", "08P01"); in_error_ = true; return; }
    uint16_t nrfmts = ntohs(*reinterpret_cast<const uint16_t *>(body.data() + pos));
    pos += 2;
    std::vector<int16_t> result_fmts(nrfmts);
    for (uint16_t i = 0; i < nrfmts; i++)
    {
        if (pos + 2 > body.size()) { enqueue_error("malformed Bind rfmt", "08P01"); in_error_ = true; return; }
        result_fmts[i] = (int16_t)ntohs(*reinterpret_cast<const uint16_t *>(body.data() + pos));
        pos += 2;
    }

    auto portal = std::make_shared<PortalEntry>();
    portal->prep_name = stmt_name;
    portal->bind_values = std::move(values);
    portal->result_formats = std::move(result_fmts);

    // If the prepared statement was deferred, pre-compute the inlined SQL and prepare it
    // now so that Describe can return proper column types/names for SELECT-style queries.
    if (!prep->stmt)
    {
        try
        {
            std::string inlined = inline_parameters(prep->query, portal->bind_values);
            PDEBUG << "bind inlined: " << inlined;
            auto reprep = connection_->Prepare(inlined);
            if (reprep && !reprep->HasError())
            {
                auto stype = reprep->GetStatementType();
                if (stype == duckdb::StatementType::SELECT_STATEMENT ||
                    stype == duckdb::StatementType::EXPLAIN_STATEMENT)
                {
                    auto &names = reprep->GetNames();
                    auto &types = reprep->GetTypes();
                    portal->result_columns.clear();
                    for (idx_t i = 0; i < names.size(); i++)
                    {
                        ColumnDesc c;
                        c.name = names[i];
                        c.logical_type = types[i];
                        c.col_num = (uint16_t)(i + 1);
                        portal->result_columns.push_back(c);
                    }
                    portal->has_result_desc = true;
                }
            }
        }
        catch (...) {}
    }

    portal_map_[portal_name] = portal;
    enqueue_bind_complete();
}

void PGSession::handle_describe(const std::vector<char> &body)
{
    if (body.size() < 2) { enqueue_error("malformed Describe", "08P01"); in_error_ = true; return; }
    char kind = body[0];
    std::string name(body.data() + 1, body.size() - 2); // minus trailing \0

    if (kind == 'S')
    {
        auto it = prep_map_.find(name);
        if (it == prep_map_.end())
        {
            enqueue_error("prepared statement \"" + name + "\" does not exist", "26000");
            in_error_ = true;
            return;
        }
        auto &prep = it->second;
        // ParameterDescription
        std::vector<uint32_t> oids;
        idx_t nparams = 0;
        if (prep->stmt)
        {
            auto expected = prep->stmt->GetExpectedParameterTypes();
            nparams = expected.size();
            if (nparams < prep->param_type_oids.size())
                nparams = prep->param_type_oids.size();
        }
        oids.reserve(nparams);
        for (idx_t i = 0; i < nparams; i++)
        {
            if (i < prep->param_type_oids.size() && prep->param_type_oids[i] != 0)
                oids.push_back(prep->param_type_oids[i]);
            else
                oids.push_back(0); // unknown
        }
        enqueue_parameter_description(oids);
        // RowDescription or NoData
        if (prep->stmt && prep->stmt->GetStatementType() == duckdb::StatementType::SELECT_STATEMENT)
        {
            std::vector<ColumnDesc> cols;
            auto &names = prep->stmt->GetNames();
            auto &types = prep->stmt->GetTypes();
            for (idx_t i = 0; i < names.size(); i++)
            {
                ColumnDesc c;
                c.name = names[i];
                c.logical_type = types[i];
                c.col_num = i + 1;
                cols.push_back(c);
            }
            enqueue_row_description(cols);
        }
        else
        {
            enqueue_no_data();
        }
    }
    else if (kind == 'P')
    {
        auto it = portal_map_.find(name);
        if (it == portal_map_.end())
        {
            enqueue_error("portal \"" + name + "\" does not exist", "34000");
            in_error_ = true;
            return;
        }
        auto &portal = it->second;
        auto prep_it = prep_map_.find(portal->prep_name);
        if (prep_it == prep_map_.end())
        {
            enqueue_error("prepared statement for portal missing", "26000");
            in_error_ = true;
            return;
        }
        auto &prep = prep_it->second;
        if (portal->has_result_desc && !portal->result_columns.empty())
        {
            enqueue_row_description(portal->result_columns, &portal->result_formats);
        }
        else if (prep->stmt && prep->stmt->GetStatementType() == duckdb::StatementType::SELECT_STATEMENT)
        {
            std::vector<ColumnDesc> cols;
            auto &names = prep->stmt->GetNames();
            auto &types = prep->stmt->GetTypes();
            for (idx_t i = 0; i < names.size(); i++)
            {
                ColumnDesc c;
                c.name = names[i];
                c.logical_type = types[i];
                c.col_num = (uint16_t)(i + 1);
                cols.push_back(c);
            }
            portal->result_columns = cols;
            portal->has_result_desc = true;
            enqueue_row_description(cols, &portal->result_formats);
        }
        else
        {
            enqueue_no_data();
        }
    }
    else
    {
        enqueue_error("unknown describe kind", "08P01");
        in_error_ = true;
    }
}

// Escape a value as a PostgreSQL string literal.
static std::string escape_sql_string(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('\'');
    for (char c : s)
    {
        if (c == '\'') out.push_back('\'');
        out.push_back(c);
    }
    out.push_back('\'');
    return out;
}

// Render a duckdb::Value as a SQL literal suitable for direct inlining.
static std::string value_to_sql_literal(const duckdb::Value &v)
{
    if (v.IsNull()) return "NULL";
    switch (v.type().id())
    {
    case duckdb::LogicalTypeId::BOOLEAN:
        return v.GetValue<bool>() ? "TRUE" : "FALSE";
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
    case duckdb::LogicalTypeId::HUGEINT:
    case duckdb::LogicalTypeId::UHUGEINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DECIMAL:
        return v.ToString();
    default:
        return escape_sql_string(v.ToString());
    }
}

// Replace $1, $2, ... placeholders in the SQL with the supplied values. Respects
// single-quote strings, double-quote identifiers, dollar-quoted strings, and SQL comments.
static std::string inline_parameters(const std::string &sql, const duckdb::vector<duckdb::Value> &values)
{
    std::string out;
    out.reserve(sql.size());
    size_t i = 0;
    while (i < sql.size())
    {
        char c = sql[i];
        if (c == '\'')
        {
            size_t j = i + 1;
            while (j < sql.size())
            {
                if (sql[j] == '\'' && j + 1 < sql.size() && sql[j + 1] == '\'')
                { j += 2; continue; }
                if (sql[j] == '\'') { j++; break; }
                j++;
            }
            out.append(sql, i, j - i);
            i = j;
            continue;
        }
        if (c == '"')
        {
            size_t j = i + 1;
            while (j < sql.size() && sql[j] != '"') j++;
            if (j < sql.size()) j++;
            out.append(sql, i, j - i);
            i = j;
            continue;
        }
        if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-')
        {
            size_t j = i;
            while (j < sql.size() && sql[j] != '\n') j++;
            out.append(sql, i, j - i);
            i = j;
            continue;
        }
        if (c == '$' && i + 1 < sql.size() && std::isdigit((unsigned char)sql[i + 1]))
        {
            size_t j = i + 1;
            size_t idx_start = j;
            while (j < sql.size() && std::isdigit((unsigned char)sql[j])) j++;
            int n = std::stoi(sql.substr(idx_start, j - idx_start));
            if (n >= 1 && (size_t)n <= values.size())
            {
                out += value_to_sql_literal(values[n - 1]);
                i = j;
                continue;
            }
        }
        out.push_back(c);
        i++;
    }
    return out;
}

void PGSession::handle_execute(const std::vector<char> &body)
{
    size_t pos = 0;
    auto read_cstr = [&](std::string &out)
    {
        size_t start = pos;
        while (pos < body.size() && body[pos] != '\0') pos++;
        out.assign(body.data() + start, pos - start);
        if (pos < body.size()) pos++;
    };
    std::string portal_name;
    read_cstr(portal_name);
    if (pos + 4 > body.size()) { enqueue_error("malformed Execute", "08P01"); in_error_ = true; return; }
    int32_t max_rows = (int32_t)ntohl(*reinterpret_cast<const uint32_t *>(body.data() + pos));
    (void)max_rows;

    auto it = portal_map_.find(portal_name);
    if (it == portal_map_.end())
    {
        enqueue_error("portal \"" + portal_name + "\" does not exist", "34000");
        in_error_ = true;
        return;
    }
    auto &portal = it->second;
    auto prep_it = prep_map_.find(portal->prep_name);
    if (prep_it == prep_map_.end())
    {
        enqueue_error("prepared statement for portal missing", "26000");
        in_error_ = true;
        return;
    }
    auto &prep = prep_it->second;

    duckdb::unique_ptr<duckdb::QueryResult> qres;
    duckdb::StatementType stmt_type = duckdb::StatementType::SELECT_STATEMENT;
    try
    {
        if (prep->stmt)
        {
            qres = prep->stmt->Execute(portal->bind_values, false);
            stmt_type = prep->stmt->GetStatementType();
        }
        else
        {
            // Deferred: inline the parameters directly into the SQL text and run as a
            // plain query. This side-steps DuckDB's parameter-type inference issues for
            // queries like "col + $1" where the type of $1 cannot be inferred up-front.
            std::string inlined = inline_parameters(prep->query, portal->bind_values);
            PDEBUG << "execute inlined: " << inlined;
            qres = connection_->SendQuery(inlined);
            if (qres)
                stmt_type = qres->statement_type;
        }
    }
    catch (std::exception &e)
    {
        enqueue_error(e.what(), "XX000");
        in_error_ = true;
        return;
    }
    if (qres->HasError())
    {
        std::string err = qres->GetError();
        std::string elower = boost::algorithm::to_lower_copy(err);
        if (elower.find("cannot start a transaction within a transaction") != std::string::npos)
        {
            enqueue_command_complete("BEGIN");
            return;
        }
        if (elower.find("cannot commit - there is no transaction active") != std::string::npos)
        {
            enqueue_command_complete("COMMIT");
            return;
        }
        if (elower.find("cannot rollback - there is no transaction active") != std::string::npos)
        {
            enqueue_command_complete("ROLLBACK");
            return;
        }
        if (elower.find("current transaction is aborted") != std::string::npos)
        {
            // Auto-recover by rolling back, then retry once.
            try { connection_->Query("ROLLBACK;"); } catch (...) {}
            try
            {
                if (prep->stmt)
                    qres = prep->stmt->Execute(portal->bind_values, false);
                else
                {
                    std::string inlined = inline_parameters(prep->query, portal->bind_values);
                    qres = connection_->SendQuery(inlined);
                }
            }
            catch (...) {}
            if (!qres || qres->HasError())
            {
                enqueue_error(qres ? qres->GetError() : std::string("retry failed"), "XX000");
                in_error_ = true;
                return;
            }
        }
        else
        {
            enqueue_error(err, "XX000");
            in_error_ = true;
            return;
        }
    }

    bool is_select =
        (stmt_type == duckdb::StatementType::SELECT_STATEMENT) ||
        (stmt_type == duckdb::StatementType::EXPLAIN_STATEMENT) ||
        (stmt_type == duckdb::StatementType::EXECUTE_STATEMENT) ||
        (stmt_type == duckdb::StatementType::CALL_STATEMENT);

    idx_t row_count = 0;
    if (is_select)
    {
        std::vector<int16_t> fmts(qres->ColumnCount(), 0);
        if (portal->result_formats.size() == 1)
            fmts.assign(qres->ColumnCount(), portal->result_formats[0]);
        else if (!portal->result_formats.empty())
        {
            for (size_t i = 0; i < fmts.size() && i < portal->result_formats.size(); i++)
                fmts[i] = portal->result_formats[i];
        }
        while (true)
        {
            auto chunk = qres->Fetch();
            if (!chunk || chunk->size() == 0) break;
            for (idx_t r = 0; r < chunk->size(); r++)
                enqueue_data_row_chunk(*chunk, r, fmts);
            row_count += chunk->size();
        }
    }
    else
    {
        auto chunk = qres->Fetch();
        if (chunk && chunk->size() > 0 && chunk->ColumnCount() > 0)
        {
            try
            {
                row_count = (idx_t)chunk->GetValue(0, 0).GetValue<int64_t>();
            }
            catch (...) { row_count = 0; }
        }
        while (chunk && chunk->size() > 0)
            chunk = qres->Fetch();
    }
    enqueue_command_complete(statement_tag_for(stmt_type, row_count));
}

void PGSession::handle_close(const std::vector<char> &body)
{
    if (body.size() < 2) { enqueue_error("malformed Close", "08P01"); in_error_ = true; return; }
    char kind = body[0];
    std::string name(body.data() + 1, body.size() - 2);
    if (kind == 'S') prep_map_.erase(name);
    else if (kind == 'P') portal_map_.erase(name);
    enqueue_close_complete();
}

void PGSession::handle_sync()
{
    in_error_ = false;
    enqueue_ready_for_query();
    flush_output();
}

void PGSession::handle_flush()
{
    flush_output();
}

// --- message appenders ---
void PGSession::enqueue_parse_complete()
{
    std::vector<char> m = {'1', 0, 0, 0, 4};
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}
void PGSession::enqueue_bind_complete()
{
    std::vector<char> m = {'2', 0, 0, 0, 4};
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}
void PGSession::enqueue_close_complete()
{
    std::vector<char> m = {'3', 0, 0, 0, 4};
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}
void PGSession::enqueue_portal_suspended()
{
    std::vector<char> m = {'s', 0, 0, 0, 4};
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}
void PGSession::enqueue_empty_query_response()
{
    std::vector<char> m = {'I', 0, 0, 0, 4};
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}
void PGSession::enqueue_no_data()
{
    std::vector<char> m = {'n', 0, 0, 0, 4};
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}

void PGSession::enqueue_parameter_description(const std::vector<uint32_t> &param_oids)
{
    std::vector<char> msg;
    msg.push_back('t');
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0);
    append_u16(msg, (uint16_t)param_oids.size());
    for (auto oid : param_oids)
        append_u32(msg, oid);
    uint32_t total = msg.size() - len_pos;
    uint32_t n = htonl(total);
    std::memcpy(msg.data() + len_pos, &n, 4);
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

void PGSession::enqueue_row_description(const std::vector<ColumnDesc> &columns,
                                        const std::vector<int16_t> *result_formats)
{
    std::vector<char> msg;
    msg.push_back('T');
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0);
    append_u16(msg, (uint16_t)columns.size());
    for (size_t i = 0; i < columns.size(); i++)
    {
        const auto &col = columns[i];
        append_cstr(msg, col.name);
        append_u32(msg, col.table_oid);
        append_u16(msg, col.col_num);
        uint32_t oid = pg_type_oid(col.logical_type);
        append_u32(msg, oid);
        append_i16(msg, pg_type_len(oid));
        append_i32(msg, -1); // typmod
        int16_t fmt = 0;
        if (result_formats)
        {
            if (result_formats->size() == 1) fmt = (*result_formats)[0];
            else if (i < result_formats->size()) fmt = (*result_formats)[i];
        }
        append_i16(msg, fmt);
    }
    uint32_t total = msg.size() - len_pos;
    uint32_t n = htonl(total);
    std::memcpy(msg.data() + len_pos, &n, 4);
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

// Format a duckdb::Value into a text PG wire representation
static std::string value_to_pg_text(const duckdb::Value &v)
{
    if (v.IsNull()) return {};
    switch (v.type().id())
    {
    case duckdb::LogicalTypeId::BOOLEAN:
        return v.GetValue<bool>() ? std::string("t") : std::string("f");
    default:
        return v.ToString();
    }
}

void PGSession::enqueue_data_row_chunk(duckdb::DataChunk &chunk, idx_t row_idx,
                                       const std::vector<int16_t> &formats)
{
    std::vector<char> msg;
    msg.push_back('D');
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0);
    uint16_t ncols = (uint16_t)chunk.ColumnCount();
    append_u16(msg, ncols);
    for (idx_t c = 0; c < ncols; c++)
    {
        auto val = chunk.GetValue(c, row_idx);
        int16_t fmt = 0;
        if (!formats.empty())
        {
            if (formats.size() == 1) fmt = formats[0];
            else if (c < formats.size()) fmt = formats[c];
        }
        if (val.IsNull())
        {
            append_i32(msg, -1);
            continue;
        }
        if (fmt == 0)
        {
            std::string txt = value_to_pg_text(val);
            append_i32(msg, (int32_t)txt.size());
            msg.insert(msg.end(), txt.begin(), txt.end());
        }
        else
        {
            // Binary format: emit common numeric types in network order.
            switch (val.type().id())
            {
            case duckdb::LogicalTypeId::BOOLEAN:
            {
                append_i32(msg, 1);
                msg.push_back(val.GetValue<bool>() ? 1 : 0);
                break;
            }
            case duckdb::LogicalTypeId::SMALLINT:
            {
                append_i32(msg, 2);
                append_i16(msg, val.GetValue<int16_t>());
                break;
            }
            case duckdb::LogicalTypeId::INTEGER:
            {
                append_i32(msg, 4);
                append_i32(msg, val.GetValue<int32_t>());
                break;
            }
            case duckdb::LogicalTypeId::BIGINT:
            {
                append_i32(msg, 8);
                int64_t x = val.GetValue<int64_t>();
                uint64_t u = (uint64_t)x;
                uint32_t hi = htonl((uint32_t)(u >> 32));
                uint32_t lo = htonl((uint32_t)(u & 0xFFFFFFFFULL));
                msg.insert(msg.end(), reinterpret_cast<char *>(&hi), reinterpret_cast<char *>(&hi) + 4);
                msg.insert(msg.end(), reinterpret_cast<char *>(&lo), reinterpret_cast<char *>(&lo) + 4);
                break;
            }
            case duckdb::LogicalTypeId::FLOAT:
            {
                append_i32(msg, 4);
                float f = val.GetValue<float>();
                uint32_t i;
                std::memcpy(&i, &f, 4);
                append_u32(msg, i);
                break;
            }
            case duckdb::LogicalTypeId::DOUBLE:
            {
                append_i32(msg, 8);
                double d = val.GetValue<double>();
                uint64_t u;
                std::memcpy(&u, &d, 8);
                uint32_t hi = htonl((uint32_t)(u >> 32));
                uint32_t lo = htonl((uint32_t)(u & 0xFFFFFFFFULL));
                msg.insert(msg.end(), reinterpret_cast<char *>(&hi), reinterpret_cast<char *>(&hi) + 4);
                msg.insert(msg.end(), reinterpret_cast<char *>(&lo), reinterpret_cast<char *>(&lo) + 4);
                break;
            }
            default:
            {
                // Fallback: treat as text bytes
                std::string txt = value_to_pg_text(val);
                append_i32(msg, (int32_t)txt.size());
                msg.insert(msg.end(), txt.begin(), txt.end());
                break;
            }
            }
        }
    }
    uint32_t total = msg.size() - len_pos;
    uint32_t n = htonl(total);
    std::memcpy(msg.data() + len_pos, &n, 4);
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

void PGSession::enqueue_data_row_text(const std::vector<std::string> &values)
{
    std::vector<char> msg;
    msg.push_back('D');
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0);
    append_u16(msg, (uint16_t)values.size());
    for (auto &v : values)
    {
        append_i32(msg, (int32_t)v.size());
        msg.insert(msg.end(), v.begin(), v.end());
    }
    uint32_t total = msg.size() - len_pos;
    uint32_t n = htonl(total);
    std::memcpy(msg.data() + len_pos, &n, 4);
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

void PGSession::enqueue_command_complete(const std::string &tag)
{
    std::vector<char> msg;
    msg.push_back('C');
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0);
    append_cstr(msg, tag);
    uint32_t total = msg.size() - len_pos;
    uint32_t n = htonl(total);
    std::memcpy(msg.data() + len_pos, &n, 4);
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

void PGSession::enqueue_ready_for_query()
{
    std::vector<char> m;
    m.push_back('Z');
    append_u32(m, 5);
    m.push_back(tx_status_);
    out_buf_.insert(out_buf_.end(), m.begin(), m.end());
}

void PGSession::enqueue_error(const std::string &message, const std::string &sqlstate)
{
    std::vector<char> body;
    body.push_back('S');
    body.insert(body.end(), {'E', 'R', 'R', 'O', 'R', '\0'});
    body.push_back('V');
    body.insert(body.end(), {'E', 'R', 'R', 'O', 'R', '\0'});
    body.push_back('C');
    body.insert(body.end(), sqlstate.begin(), sqlstate.end());
    body.push_back('\0');
    body.push_back('M');
    body.insert(body.end(), message.begin(), message.end());
    body.push_back('\0');
    body.push_back('\0');

    uint32_t len = 4 + (uint32_t)body.size();
    std::vector<char> msg;
    msg.push_back('E');
    append_u32(msg, len);
    msg.insert(msg.end(), body.begin(), body.end());
    out_buf_.insert(out_buf_.end(), msg.begin(), msg.end());
}

void PGSession::flush_output()
{
    std::lock_guard<std::mutex> lg(write_mtx_);
    if (out_buf_.empty()) return;
    boost::system::error_code ec;
    asio::write(socket_, asio::buffer(out_buf_), ec);
    if (ec) PDEBUG << "write error: " << ec.message();
    out_buf_.clear();
}

void PGSession::process_materialized_result(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result,
                                            const std::string &original_query)
{
    (void)original_query;
    // (legacy, unused path); kept for header compatibility
    (void)result;
}

std::string PGSession::statement_tag_for(duckdb::StatementType t, idx_t row_count) const
{
    switch (t)
    {
    case duckdb::StatementType::SELECT_STATEMENT:
        return "SELECT " + std::to_string(row_count);
    case duckdb::StatementType::INSERT_STATEMENT:
        return "INSERT 0 " + std::to_string(row_count);
    case duckdb::StatementType::UPDATE_STATEMENT:
        return "UPDATE " + std::to_string(row_count);
    case duckdb::StatementType::DELETE_STATEMENT:
        return "DELETE " + std::to_string(row_count);
    case duckdb::StatementType::EXPLAIN_STATEMENT:
        return "EXPLAIN";
    case duckdb::StatementType::COPY_STATEMENT:
        return "COPY " + std::to_string(row_count);
    case duckdb::StatementType::CREATE_STATEMENT:
        return "CREATE";
    case duckdb::StatementType::DROP_STATEMENT:
        return "DROP";
    case duckdb::StatementType::ALTER_STATEMENT:
        return "ALTER";
    case duckdb::StatementType::TRANSACTION_STATEMENT:
        return "BEGIN";
    case duckdb::StatementType::SET_STATEMENT:
        return "SET";
    case duckdb::StatementType::PRAGMA_STATEMENT:
        return "PRAGMA";
    case duckdb::StatementType::PREPARE_STATEMENT:
        return "PREPARE";
    case duckdb::StatementType::EXECUTE_STATEMENT:
        // EXECUTE wraps an underlying statement; report SELECT n for SELECT bodies.
        return "SELECT " + std::to_string(row_count);
    default:
        return "OK";
    }
}

// Rewrite some common pg_catalog / system / JDBC-bootstrap queries that
// DuckDB does not support natively, into queries DuckDB can execute.
std::string PGSession::rewrite_query(const std::string &query) const
{
    std::string q = boost::algorithm::trim_copy(query);
    if (q.empty()) return q;

    // Strip trailing ; for comparison
    std::string cmp = q;
    while (!cmp.empty() && (cmp.back() == ';' || std::isspace((unsigned char)cmp.back())))
        cmp.pop_back();

    // Hardcoded minimal responses for JDBC / psql bootstrap queries
    // These will be recognised by simple_query via translation to equivalent SELECT.
    if (boost::algorithm::iequals(cmp,
        "SELECT reset_val FROM pg_settings WHERE name='polar_compatibility_mode'"))
    {
        return "SELECT 'pg' AS reset_val";
    }
    if (boost::algorithm::iequals(cmp, "SHOW TRANSACTION ISOLATION LEVEL"))
    {
        return "SELECT 'read committed' AS transaction_isolation";
    }
    if (boost::algorithm::iequals(cmp, "SHOW transaction_isolation"))
    {
        return "SELECT 'read committed' AS transaction_isolation";
    }
    if (boost::algorithm::iequals(cmp, "SHOW standard_conforming_strings"))
    {
        return "SELECT 'on' AS standard_conforming_strings";
    }
    if (boost::algorithm::iequals(cmp, "SHOW server_version"))
    {
        return "SELECT '14.0 (PostDuck)' AS server_version";
    }
    if (boost::algorithm::iequals(cmp, "SELECT current_schema()"))
    {
        return "SELECT 'main' AS current_schema";
    }
    if (boost::algorithm::iequals(cmp, "SELECT version()"))
    {
        return "SELECT 'PostgreSQL 14.0 (PostDuck on DuckDB) on x86_64-pc-linux-gnu' AS version";
    }

    // Rewrite JDBC DatabaseMetaData queries that rely on pg_description / regclass /
    // other PG catalog features DuckDB lacks. We match on characteristic prefixes.
    // getTables: returns a synthetic result from duckdb_tables().
    if (cmp.find("AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME") != std::string::npos &&
        cmp.find("pg_catalog.pg_class") != std::string::npos)
    {
        return
            "SELECT NULL AS TABLE_CAT, schema_name AS TABLE_SCHEM, table_name AS TABLE_NAME, "
            "'TABLE' AS TABLE_TYPE, NULL AS REMARKS, "
            "'' AS TYPE_CAT, '' AS TYPE_SCHEM, '' AS TYPE_NAME, "
            "'' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION "
            "FROM duckdb_tables() "
            "UNION ALL "
            "SELECT NULL, schema_name, view_name, 'VIEW', NULL, '', '', '', '', '' FROM duckdb_views() WHERE internal = false "
            "ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME";
    }
    // getSchemas: JDBC uses "SELECT nspname AS TABLE_SCHEM, NULL AS TABLE_CATALOG FROM pg_catalog.pg_namespace"
    if (cmp.find("AS TABLE_SCHEM") != std::string::npos &&
        cmp.find("pg_catalog.pg_namespace") != std::string::npos &&
        cmp.find("TABLE_NAME") == std::string::npos)
    {
        return "SELECT schema_name AS TABLE_SCHEM, catalog_name AS TABLE_CATALOG FROM duckdb_schemas() ORDER BY TABLE_SCHEM";
    }
    // getColumns often queries pg_attribute with complicated joins; provide a minimal fallback.
    if (cmp.find("pg_catalog.pg_attribute") != std::string::npos &&
        cmp.find("pg_catalog.pg_class") != std::string::npos &&
        cmp.find("a.attname") != std::string::npos)
    {
        return
            "SELECT NULL AS TABLE_CAT, table_schema AS TABLE_SCHEM, table_name AS TABLE_NAME, "
            "column_name AS COLUMN_NAME, 0 AS DATA_TYPE, data_type AS TYPE_NAME, "
            "NULL AS COLUMN_SIZE, NULL AS BUFFER_LENGTH, NULL AS DECIMAL_DIGITS, 10 AS NUM_PREC_RADIX, "
            "(CASE WHEN is_nullable='YES' THEN 1 ELSE 0 END) AS NULLABLE, NULL AS REMARKS, "
            "column_default AS COLUMN_DEF, NULL AS SQL_DATA_TYPE, NULL AS SQL_DATETIME_SUB, "
            "NULL AS CHAR_OCTET_LENGTH, ordinal_position AS ORDINAL_POSITION, "
            "is_nullable AS IS_NULLABLE, NULL AS SCOPE_CATALOG, NULL AS SCOPE_SCHEMA, "
            "NULL AS SCOPE_TABLE, NULL AS SOURCE_DATA_TYPE, 'NO' AS IS_AUTOINCREMENT, "
            "'NO' AS IS_GENERATEDCOLUMN "
            "FROM information_schema.columns "
            "ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";
    }
    // Intercept transaction-control commands and let DuckDB's auto-txn behaviour
    // handle things. We track the desired state via tx_status_ in handle_simple_query.
    // Here we just normalise them into forms DuckDB accepts.
    {
        std::string lower = boost::algorithm::to_lower_copy(cmp);
        // SHOW transaction_isolation / SHOW TRANSACTION ISOLATION LEVEL already handled.
        // Strip PG-specific BEGIN options like "BEGIN READ WRITE" or "BEGIN ISOLATION LEVEL READ COMMITTED"
        if (boost::algorithm::starts_with(lower, "begin") || boost::algorithm::starts_with(lower, "start transaction"))
        {
            return "BEGIN;";
        }
        if (lower == "commit" || lower == "commit transaction" || lower == "commit work" ||
            boost::algorithm::starts_with(lower, "commit "))
        {
            return "COMMIT;";
        }
        if (lower == "rollback" || lower == "rollback transaction" || lower == "rollback work" ||
            boost::algorithm::starts_with(lower, "rollback "))
        {
            return "ROLLBACK;";
        }
    }
    // Intercept SET commands that DuckDB doesn't know about (from PG clients).
    // Accept them as no-ops. This is important for JDBC which issues SET extra_float_digits=3,
    // SET application_name, etc. on connect.
    {
        std::string lower = boost::algorithm::to_lower_copy(cmp);
        if (boost::algorithm::starts_with(lower, "set "))
        {
            // Extract the parameter name (first token after SET, optionally 'session '/'local ')
            std::string rest = boost::algorithm::trim_left_copy(cmp.substr(4));
            std::string lrest = boost::algorithm::to_lower_copy(rest);
            if (boost::algorithm::starts_with(lrest, "session "))
                rest = boost::algorithm::trim_left_copy(rest.substr(8));
            else if (boost::algorithm::starts_with(lrest, "local "))
                rest = boost::algorithm::trim_left_copy(rest.substr(6));
            // first identifier: tokens until '=' or whitespace
            size_t end = 0;
            while (end < rest.size() && rest[end] != '=' && !std::isspace((unsigned char)rest[end])) end++;
            std::string param = rest.substr(0, end);
            std::string pl = boost::algorithm::to_lower_copy(param);
            // DuckDB-understood SETs we let through
            static const std::set<std::string> duckdb_settings = {
                "timezone", "search_path", "memory_limit", "threads",
                "temp_directory", "allow_unsigned_extensions", "enable_progress_bar",
                "errors_as_json", "disabled_optimizers"
            };
            if (duckdb_settings.count(pl) == 0)
            {
                return "SELECT 1 WHERE FALSE;";
            }
        }
        // RESET
        if (boost::algorithm::starts_with(lower, "reset "))
        {
            return "SELECT 1 WHERE FALSE;";
        }
    }
    // Rewrite pgbench's multi-drop into separate statements: DuckDB only supports one object per DROP.
    {
        std::string lower = boost::algorithm::to_lower_copy(cmp);
        if (boost::algorithm::starts_with(lower, "drop table"))
        {
            std::string rest = cmp.substr(std::string("drop table").size());
            std::string rest_trim = boost::algorithm::trim_left_copy(rest);
            bool if_exists = false;
            std::string lower_rest = boost::algorithm::to_lower_copy(rest_trim);
            if (boost::algorithm::starts_with(lower_rest, "if exists"))
            {
                if_exists = true;
                rest_trim = boost::algorithm::trim_left_copy(rest_trim.substr(9));
            }
            std::vector<std::string> parts;
            boost::algorithm::split(parts, rest_trim, boost::is_any_of(","));
            if (parts.size() > 1)
            {
                std::string out;
                for (auto &p : parts)
                {
                    std::string name = boost::algorithm::trim_copy(p);
                    if (name.empty()) continue;
                    out += "DROP TABLE ";
                    if (if_exists) out += "IF EXISTS ";
                    out += name + "; ";
                }
                return out;
            }
        }
        // Rewrite multi-table TRUNCATE: "truncate table a, b, c"
        if (boost::algorithm::starts_with(lower, "truncate"))
        {
            std::string rest = cmp.substr(std::string("truncate").size());
            std::string rest_trim = boost::algorithm::trim_left_copy(rest);
            std::string lrest = boost::algorithm::to_lower_copy(rest_trim);
            if (boost::algorithm::starts_with(lrest, "table"))
            {
                rest_trim = boost::algorithm::trim_left_copy(rest_trim.substr(5));
            }
            std::vector<std::string> parts;
            boost::algorithm::split(parts, rest_trim, boost::is_any_of(","));
            if (parts.size() > 1)
            {
                std::string out;
                for (auto &p : parts)
                {
                    std::string name = boost::algorithm::trim_copy(p);
                    if (name.empty()) continue;
                    out += "DELETE FROM " + name + "; ";
                }
                return out;
            }
        }
    }
    return q;
}

PGSession::~PGSession()
{
    if (backend_pid_ != 0)
    {
        std::lock_guard<std::mutex> lg(sessions_mtx);
        sessions_map.erase(backend_pid_);
        sessions_secret.erase(backend_pid_);
    }
}

void PGSession::Cancel()
{
    PINFO << "Cancelling session pid=" << backend_pid_;
    try
    {
        if (connection_)
            connection_->Interrupt();
    }
    catch (std::exception &e)
    {
        PERROR << "Error while cancelling session: " << e.what();
    }
}
