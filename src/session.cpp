#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#include <boost/thread.hpp>
#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>

#include "session.hpp"
#include "log.hpp"
#include "db.hpp"

#include <memory>
#include <set>

using boost::asio::ip::tcp;
static boost::asio::thread_pool thread_pool_(4); // 4 threads in the pool
static std::string datadir = ".";

std::unordered_map<std::string, uint32_t> duckdb_to_pg_type = {
    {"BOOLEAN", 16},     // PG: bool
    {"TINYINT", 21},     // PG: int2
    {"SMALLINT", 21},    // PG: int2
    {"INTEGER", 23},     // PG: int4
    {"BIGINT", 20},      // PG: int8
    {"FLOAT", 700},      // PG: float4
    {"DOUBLE", 701},     // PG: float8
    {"VARCHAR", 1043},   // PG: varchar
    {"CHAR", 1042},      // PG: bpchar
    {"DATE", 1082},      // PG: date
    {"TIME", 1083},      // PG: time
    {"TIMESTAMP", 1114}, // PG: timestamp
    {"BLOB", 17},        // PG: bytea
    {"DECIMAL", 1700},   // PG: numeric
    // 添加更多类型映射...
};

void set_data_directory(const std::string &dir)
{
    datadir = dir;
    if (datadir.back() == '/')
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

// 解析启动参数
void PGSession::parse_startup_params(const char *data, size_t length)
{
    size_t pos = 0;
    // protocal version
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
    std::string startup;
    for (const auto &param : startup_params_)
    {
        startup += param.first + "=" + param.second + ", ";
    }
    PDEBUG << startup;
}

// SSL协商处理
void PGSession::handle_ssl_negotiation()
{
    asio::async_read(socket_, asio::buffer(data_, 8),
                     [self = shared_from_this()](boost::system::error_code ec, size_t)
                     {
                         if (!ec)
                         {
                             // 拒绝SSL或非SSL连接
                             if (memcmp(self->data_ + 4, "\x04\xD2\x16\x2F", 4) == 0)
                                 asio::write(self->socket_, asio::buffer("N", 1));

                             self->handle_startup();
                         }
                     });
}

// 处理启动消息
void PGSession::handle_startup()
{
    asio::async_read(socket_, asio::buffer(data_, 4),
                     [self = shared_from_this()](boost::system::error_code ec, size_t)
                     {
                         if (!ec)
                         {
                             self->length_ = ntohl(*reinterpret_cast<uint32_t *>(self->data_));
                             asio::async_read(self->socket_, asio::buffer(self->data_, self->length_ - 4),
                                              [self](boost::system::error_code ec, size_t)
                                              {
                                                  if (!ec)
                                                  {
                                                      // 解析启动包 (version, params...)
                                                      self->parse_startup_params(self->data_, self->length_ - 4);
                                                      self->handle_authentication();
                                                  }
                                              });
                         }
                     });
}

// 处理认证
void PGSession::handle_authentication()
{
    // 这里简化认证过程，实际应根据客户端信息进行认证
    send_auth_ok();
}

// 发送认证成功响应
void PGSession::send_auth_ok()
{
    std::vector<char> auth_ok = {'R', 0, 0, 0, 8, 0, 0, 0, 0};
    asio::write(socket_, asio::buffer(auth_ok));

    // 发送参数状态
    send_parameter_status("client_encoding", "UTF8");
    send_parameter_status("DateStyle", "ISO");

    // 发送BackendKeyData（简化版）
    std::vector<char> backend_key = {'K', 0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 2};
    asio::write(socket_, asio::buffer(backend_key));

    // 加载默认数据库
    std::string query = "ATTACH '" + datadir + "/" + startup_params_["database"] + ".db';";
    connection_->Query(query);
    query = "USE " + startup_params_["database"] + ";";
    connection_->Query(query);

    // 发送ReadyForQuery
    std::vector<char> ready = {'Z', 0, 0, 0, 5, 'I'};
    asio::write(socket_, asio::buffer(ready));

    // 进入查询处理循环
    handle_query();
}

// 发送参数状态
void PGSession::send_parameter_status(const std::string &name, const std::string &value)
{
    std::vector<char> param_msg;
    param_msg.push_back('S');

    uint32_t len = 4 + name.size() + 1 + value.size() + 1;
    uint32_t net_len = htonl(len);

    param_msg.insert(param_msg.end(),
                     reinterpret_cast<char *>(&net_len),
                     reinterpret_cast<char *>(&net_len) + 4);

    param_msg.insert(param_msg.end(), name.begin(), name.end());
    param_msg.push_back('\0');
    param_msg.insert(param_msg.end(), value.begin(), value.end());
    param_msg.push_back('\0');

    asio::write(socket_, asio::buffer(param_msg));
}

// 处理查询
void PGSession::handle_query()
{
    asio::async_read(socket_, asio::buffer(data_, 1),
                     [self = shared_from_this()](boost::system::error_code ec, size_t)
                     {
                         if (!ec)
                         {
                             switch (self->data_[0])
                             {
                             case 'Q': // 简单查询
                                 self->handle_simple_query();
                                 break;
                             case 'P': // 扩展查询
                                 // self->handle_extended_query();
                                 break;
                             case 'X': // 终止
                                 return;
                             default:
                                 // 未知消息类型
                                 self->handle_query();
                             }
                         }
                     });
}

// 处理简单查询
void PGSession::handle_simple_query()
{
    asio::async_read(socket_, asio::buffer(data_, 4),
                     [self = shared_from_this()](boost::system::error_code ec, size_t)
                     {
                         if (!ec)
                         {
                             uint32_t msg_length = ntohl(*reinterpret_cast<uint32_t *>(self->data_));
                             // 读取查询字符串
                             msg_length = msg_length - 4;
                             self->query_.resize(msg_length);
                             asio::async_read(self->socket_, asio::buffer(self->query_),
                                              [self](boost::system::error_code ec, size_t)
                                              {
                                                  if (!ec)
                                                  {
                                                      // 处理查询并返回结果
                                                      boost::asio::post(thread_pool_,
                                                                        [self]()
                                                                        {
                                                                            self->process_query();
                                                                        });
                                                      // 继续处理下一个查询
                                                      self->handle_query();
                                                  }
                                              });
                         }
                     });
}

void PGSession::process_select(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    // 发送行描述
    std::vector<ColumnDesc> columns;
    for (idx_t i = 0; i < result->ColumnCount(); i++)
    {
        ColumnDesc col;
        col.name = result->ColumnName(i);
        col.duckdb_type = "VARCHAR"; // 简化处理
        col.table_oid = 0;           // 简化处理
        col.col_num = i + 1;
        col.type_len = -1; // 可变长度
        columns.push_back(col);
    }
    send_row_description(columns);

    // 发送数据行
    while (true)
    {
        auto chunk = result->Fetch();
        if (!chunk || chunk->size() == 0)
            break;
        for (idx_t row = 0; row < chunk->size(); row++)
        {
            std::vector<std::string> values;
            for (idx_t col = 0; col < chunk->ColumnCount(); col++)
            {
                auto value = chunk->GetValue(col, row);
                if (value.IsNull())
                    values.push_back("");
                else
                    values.push_back(value.ToString());
            }
            send_data_row(values);
        }
    }
    send_command_complete("SELECT " + std::to_string(result->RowCount()));
}

void PGSession::process_insert(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    /* INSERT oid 1*/
    std::string response = "INSERT 0 ";
    if (result->RowCount() > 0)
    {
        auto chunk = result->Fetch();
        if (chunk && chunk->size() > 0)
        {
            response += chunk->GetValue(0, 0).ToString();
        }
    }
    send_command_complete(response);
}

void PGSession::process_update(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    /* UPDATE 1*/
    std::string response = "UPDATE ";
    if (result->RowCount() > 0)
    {
        auto chunk = result->Fetch();
        if (chunk && chunk->size() > 0)
        {
            response += chunk->GetValue(0, 0).ToString();
        }
    }
    send_command_complete(response);
}

void PGSession::process_delete(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    /* DELETE 1*/
    std::string response = "DELETE ";
    if (result->RowCount() > 0)
    {
        auto chunk = result->Fetch();
        if (chunk && chunk->size() > 0)
        {
            response += chunk->GetValue(0, 0).ToString();
        }
    }
    send_command_complete(response);
}

void PGSession::process_explain(duckdb::unique_ptr<duckdb::MaterializedQueryResult> &result)
{
    // 发送行描述
    std::vector<ColumnDesc> columns;
    {
        ColumnDesc col;
        col.name = "QUERY PLAN";
        col.duckdb_type = "VARCHAR"; // 简化处理
        col.table_oid = 0;           // 简化处理
        col.col_num = 1;
        col.type_len = -1; // 可变长度
        columns.push_back(col);
    }
    send_row_description(columns);

    // 发送数据行
    while (true)
    {
        auto chunk = result->Fetch();
        if (!chunk || chunk->size() == 0)
            break;
        for (idx_t row = 0; row < chunk->size(); row++)
        {
            std::vector<std::string> values;
            {
                auto value = chunk->GetValue(1, row);
                if (value.IsNull())
                    values.push_back("");
                else
                    values.push_back(value.ToString());
            }
            send_data_row(values);
        }
    }
    send_command_complete("EXPLAIN");
}

// 处理查询并返回结果
void PGSession::process_query()
{
    std::string query(query_.begin(), query_.end() - 1); // 去掉末尾的\0

    PDEBUG << "Received query: " << query;
    if (query == "SELECT reset_val FROM pg_settings WHERE name='polar_compatibility_mode';")
    {
        // 特殊处理polar_compatibility_mode查询
        send_row_description({{"reset_val", "VARCHAR", 0, 0, -1}});
        send_data_row({"pg"});
        send_command_complete("SELECT");
    }
    else
    {
        auto result = connection_->Query(query);
        if (result->HasError())
        {
            // 发送错误响应（简化版）
            send_command_complete(result->GetError());
        }
        else
        {
            // 发送命令完成
            switch (result->statement_type)
            {
            case duckdb::StatementType::SELECT_STATEMENT:
                process_select(result);
                break;
            case duckdb::StatementType::INSERT_STATEMENT:
                process_insert(result);
                break;
            case duckdb::StatementType::UPDATE_STATEMENT:
                process_update(result);
                break;
            case duckdb::StatementType::DELETE_STATEMENT:
                process_delete(result);
                break;
            case duckdb::StatementType::EXPLAIN_STATEMENT:
                process_explain(result);
                break;
            default:
                send_command_complete("COMMAND COMPLETE");
                break;
            }
        }
    }
    // 3. 发送ReadyForQuery
    send_ready_for_query();
}

// 发送行描述
void PGSession::send_row_description(const std::vector<ColumnDesc> &columns)
{
    std::vector<uint8_t> msg;

    // 1. 消息类型 'T'
    msg.push_back('T');

    // 2. 临时占位长度字段(4字节)，后面会填充
    uint32_t len_pos = msg.size();
    msg.insert(msg.end(), 4, 0);

    // 3. 字段数量(2字节)
    uint16_t num_fields = htons(columns.size());
    msg.insert(msg.end(),
               reinterpret_cast<uint8_t *>(&num_fields),
               reinterpret_cast<uint8_t *>(&num_fields) + 2);

    // 4. 每个字段的描述
    for (const auto &col : columns)
    {
        // 字段名(以\0结尾)
        msg.insert(msg.end(), col.name.begin(), col.name.end());
        msg.push_back('\0');

        // 表OID(4字节)
        uint32_t table_oid = htonl(col.table_oid);
        msg.insert(msg.end(),
                   reinterpret_cast<uint8_t *>(&table_oid),
                   reinterpret_cast<uint8_t *>(&table_oid) + 4);

        // 列号(2字节)
        uint16_t col_num = htons(col.col_num);
        msg.insert(msg.end(),
                   reinterpret_cast<uint8_t *>(&col_num),
                   reinterpret_cast<uint8_t *>(&col_num) + 2);

        // 类型OID(4字节)
        uint32_t type_oid = htonl(duckdb_to_pg_type.at(col.duckdb_type));
        msg.insert(msg.end(),
                   reinterpret_cast<uint8_t *>(&type_oid),
                   reinterpret_cast<uint8_t *>(&type_oid) + 4);

        // 类型长度(2字节)
        int16_t type_len = htons(col.type_len);
        msg.insert(msg.end(),
                   reinterpret_cast<uint8_t *>(&type_len),
                   reinterpret_cast<uint8_t *>(&type_len) + 2);

        // 类型修饰符(4字节，通常为-1)
        int32_t type_mod = htonl(-1);
        msg.insert(msg.end(),
                   reinterpret_cast<uint8_t *>(&type_mod),
                   reinterpret_cast<uint8_t *>(&type_mod) + 4);

        // 格式码(2字节，0=文本)
        int16_t format = htons(0);
        msg.insert(msg.end(),
                   reinterpret_cast<uint8_t *>(&format),
                   reinterpret_cast<uint8_t *>(&format) + 2);
    }

    // 5. 计算并回填总长度(包括长度字段自身4字节)
    uint32_t total_len = msg.size() - len_pos;
    uint32_t net_len = htonl(total_len);
    std::copy(reinterpret_cast<uint8_t *>(&net_len),
              reinterpret_cast<uint8_t *>(&net_len) + 4,
              msg.begin() + len_pos);

    boost::asio::post(get_io_context(), [self = shared_from_this(), msg]()
                      { asio::write(self->socket_, asio::buffer(msg)); });
}

// 发送数据行
void PGSession::send_data_row(const std::vector<std::string> &values)
{
    std::vector<char> msg;
    msg.push_back('D');

    uint32_t len = 4 + 2;
    for (const auto &val : values)
    {
        len += 4 + val.size();
    }

    uint32_t net_len = htonl(len);
    msg.insert(msg.end(),
               reinterpret_cast<char *>(&net_len),
               reinterpret_cast<char *>(&net_len) + 4);

    // 字段数量
    uint16_t num_fields = htons(values.size());
    msg.insert(msg.end(),
               reinterpret_cast<char *>(&num_fields),
               reinterpret_cast<char *>(&num_fields) + 2);

    // 每个字段的值
    for (const auto &val : values)
    {
        uint32_t val_len = htonl(val.size());
        msg.insert(msg.end(),
                   reinterpret_cast<char *>(&val_len),
                   reinterpret_cast<char *>(&val_len) + 4);
        msg.insert(msg.end(), val.begin(), val.end());
    }

    boost::asio::post(get_io_context(),
                      [self = shared_from_this(), msg]()
                      {
                          asio::write(self->socket_, asio::buffer(msg));
                      });
}

// 发送命令完成
void PGSession::send_command_complete(const std::string &tag)
{
    std::vector<char> msg;
    msg.push_back('C');

    uint32_t len = 4 + tag.size() + 1;
    uint32_t net_len = htonl(len);

    msg.insert(msg.end(),
               reinterpret_cast<char *>(&net_len),
               reinterpret_cast<char *>(&net_len) + 4);
    msg.insert(msg.end(), tag.begin(), tag.end());
    msg.push_back('\0');

    boost::asio::post(get_io_context(),
                      [self = shared_from_this(), msg]()
                      {
                          asio::write(self->socket_, asio::buffer(msg));
                      });
}

// 发送ReadyForQuery
void PGSession::send_ready_for_query()
{
    boost::asio::post(get_io_context(),
                      [self = shared_from_this()]()
                      {
                          std::vector<char> ready = {'Z', 0, 0, 0, 5, 'I'};
                          asio::write(self->socket_, asio::buffer(ready));
                      });
}