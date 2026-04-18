#ifndef SESSION_HPP
#define SESSION_HPP
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <mutex>
#include <duckdb.hpp>

using boost::asio::ip::tcp;
namespace asio = boost::asio;

struct ColumnDesc
{
    std::string name;
    duckdb::LogicalType logical_type = duckdb::LogicalType::VARCHAR;
    uint32_t table_oid = 0;
    uint16_t col_num = 0;
};

// Cached prepared statement for extended query protocol
struct PreparedStatementEntry
{
    std::string query;
    duckdb::unique_ptr<duckdb::PreparedStatement> stmt;
    std::vector<uint32_t> param_type_oids;
};

// Portal: a bound prepared statement ready to execute
struct PortalEntry
{
    std::string prep_name;
    duckdb::vector<duckdb::Value> bind_values;
    std::vector<int16_t> result_formats;
    bool has_result_desc = false;
    std::vector<ColumnDesc> result_columns;
};

class PGSession : public std::enable_shared_from_this<PGSession>
{
    tcp::socket socket_;
    std::vector<char> msg_buf_;   // current message body buffer
    std::vector<char> out_buf_;   // output accumulation buffer
    std::mutex write_mtx_;        // serialize writes on socket
    // Per-session strand to serialise message handling (extended protocol must run in order).
    std::shared_ptr<boost::asio::strand<boost::asio::thread_pool::executor_type>> strand_;

    std::vector<char> startup_packet_;
    std::shared_ptr<duckdb::Connection> connection_;
    std::map<std::string, std::string> startup_params_;
    uint32_t backend_pid_ = 0;
    uint32_t backend_secret_ = 0;

    // Extended protocol state
    std::map<std::string, std::shared_ptr<PreparedStatementEntry>> prep_map_;
    std::map<std::string, std::shared_ptr<PortalEntry>> portal_map_;

    // In-transaction status
    char tx_status_ = 'I'; // 'I' idle, 'T' in transaction, 'E' failed transaction
    bool in_error_ = false; // whether we're in a failed extended protocol sequence until Sync

public:
    PGSession(tcp::socket socket, std::shared_ptr<duckdb::Connection> conn)
        : socket_(std::move(socket)), connection_(conn) {}

    ~PGSession();

    void Cancel();

    // append an ErrorResponse message to out_buf_
    void enqueue_error(const std::string &message, const std::string &sqlstate = "XX000");

    void start()
    {
        handle_ssl_negotiation();
    }

    static boost::asio::io_context &get_io_context();

private:
    void parse_startup_params(const char *data, size_t length);
    void handle_ssl_negotiation();
    void handle_startup();
    void handle_authentication();
    void send_auth_ok();
    void append_parameter_status(const std::string &name, const std::string &value);

    // Message reading loop
    void read_message();
    void dispatch_message(char msg_type, std::vector<char> &&body);

    // Simple query
    void handle_simple_query(const std::string &query);

    // Extended query protocol
    void handle_parse(const std::vector<char> &body);
    void handle_bind(const std::vector<char> &body);
    void handle_describe(const std::vector<char> &body);
    void handle_execute(const std::vector<char> &body);
    void handle_close(const std::vector<char> &body);
    void handle_sync();
    void handle_flush();

    // writers (append into out_buf_)
    void enqueue_row_description(const std::vector<ColumnDesc> &columns,
                                 const std::vector<int16_t> *result_formats = nullptr);
    void enqueue_no_data();
    void enqueue_parameter_description(const std::vector<uint32_t> &param_oids);
    void enqueue_parse_complete();
    void enqueue_bind_complete();
    void enqueue_close_complete();
    void enqueue_portal_suspended();
    void enqueue_empty_query_response();
    void enqueue_data_row_text(const std::vector<std::string> &values);
    void enqueue_data_row_chunk(duckdb::DataChunk &chunk, idx_t row_idx,
                                const std::vector<int16_t> &formats);
    void enqueue_command_complete(const std::string &tag);
    void enqueue_ready_for_query();
    void flush_output();

    void process_materialized_result(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result,
                                     const std::string &original_query);

    std::string statement_tag_for(duckdb::StatementType t, idx_t row_count) const;

    // Rewrite queries that reference pg_catalog/system info not provided by DuckDB
    std::string rewrite_query(const std::string &query) const;
};

void set_data_directory(const std::string &dir);

void init_thread_pool(size_t thread_count);
boost::asio::thread_pool& get_thread_pool();
void cleanup_thread_pool();

#endif // SESSION_HPP
