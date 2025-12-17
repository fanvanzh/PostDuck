#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <boost/program_options.hpp>
#include <boost/log/expressions.hpp>
#include <boost/algorithm/string.hpp>

#include <iostream>
#include <memory>
#include <set>

#include "session.hpp"
#include "log.hpp"
#include "db.hpp"

using boost::asio::ip::tcp;
namespace asio = boost::asio;
namespace logging = boost::log;
namespace po = boost::program_options;

class Server
{
	DB duckdb_;

public:
	Server(boost::asio::io_context &io_context, short port)
		: acceptor_(io_context, tcp::endpoint(tcp::v4(), port))
	{
		accept();
	}

private:
	void accept()
	{
		acceptor_.async_accept(
			[this](boost::system::error_code ec, tcp::socket socket)
			{
				if (!ec)
				{
					PINFO << "New connection from " << socket.remote_endpoint();
					auto session = std::make_shared<PGSession>(std::move(socket), duckdb_.get_connection());
					session->start();
				}
				else
				{
					PERROR << "Accept error: " << ec.message();
				}
				accept(); // 继续接受新连接
			});
	}

	tcp::acceptor acceptor_;
};

int main(int argc, char *argv[])
{
	try
	{
		int port = 5432;
		po::options_description desc("options");
		desc.add_options()
			("help,h", "show help message")
			("port,p", po::value<int>(), "server listen port, default is 5432")
			("log,l", po::value<std::string>(), "server log level: {TRACE, DEBUG, INFO, WARNING, ERROR, FATAL}");

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if (vm.count("help"))
		{
			std::cerr << desc << std::endl;
			return 0;
		}

		if (vm.count("port"))
		{
			port = vm["port"].as<int>();
		}

		if (vm.count("log"))
		{
			std::string log_level = vm["log"].as<std::string>();
			log_level = boost::to_upper_copy(log_level);

			if (log_level == "TRACE")
				logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::trace);
			else if (log_level == "DEBUG")
				logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::debug);
			else if (log_level == "INFO")
				logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::info);
			else if (log_level == "WARNING")
				logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::warning);
			else if (log_level == "ERROR")
				logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::error);
			else if (log_level == "FATAL")
				logging::core::get()->set_filter(logging::trivial::severity >= logging::trivial::fatal);
			else
			{
				std::cerr << "Unknown log level: " << log_level << std::endl;
				std::cerr << "Must be one of: TRACE, DEBUG, INFO, WARNING, ERROR, FATAL" << std::endl;
				return 1;
			}
		}
		PINFO << "Start on port " << port;

		Server server(PGSession::get_io_context(), port);
		PGSession::get_io_context().run();
	}
	catch (std::exception &e)
	{
		PFATAL << "Exception: " << e.what();
	}

	return 0;
}