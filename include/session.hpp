#ifndef SESSION_HPP
#define SESSION_HPP
#include <boost/asio.hpp>
#include <vector>
#include <map>
#include <string>
#include <memory>
#include <duckdb.hpp>

using boost::asio::ip::tcp;
namespace asio = boost::asio;

struct ColumnDesc
{
    std::string name;
    std::string duckdb_type;
    uint32_t table_oid = 0; // 通常可以设为0
    uint16_t col_num = 0;   // 列序号
    int16_t type_len = -1;  // 类型长度，-1表示可变长度
};

class PGSession : public std::enable_shared_from_this<PGSession>
{
    tcp::socket socket_;
    char data_[4096];
    uint32_t length_;

    std::vector<char> startup_packet_;
    std::vector<char> query_;
    std::shared_ptr<duckdb::Connection> connection_;
    std::map<std::string, std::string> startup_params_;
    uint32_t backend_pid_ = 0;
    uint32_t backend_secret_ = 0;

public:
    PGSession(tcp::socket socket, std::shared_ptr<duckdb::Connection> conn)
        : socket_(std::move(socket)), connection_(conn) {}

    ~PGSession();

    // Called when a CancelRequest targets this session
    void Cancel();

    // Send an SQL error response to the client (SQLSTATE default is '57014')
    void send_error(const std::string &message, const std::string &sqlstate = "57014");

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
    void send_parameter_status(const std::string &name, const std::string &value);
    void handle_query();
    void handle_simple_query();
    void send_row_description(const std::vector<ColumnDesc> &columns);
    void send_data_row(const std::vector<std::string> &values);
    void send_command_complete(const std::string &tag);
    void send_ready_for_query();
    void process_query();
    /* dml response */
    void process_select(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result);
    void process_insert(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result);
    void process_update(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result);
    void process_delete(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result);
    /* explain */
    void process_explain(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result);
};

void set_data_directory(const std::string &dir);
#endif // SESSION_HPP