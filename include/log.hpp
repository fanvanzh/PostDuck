#ifndef LOG_HPP
#define LOG_HPP

#include <boost/log/trivial.hpp>

#define PTRACE BOOST_LOG_TRIVIAL(trace)
#define PDEBUG BOOST_LOG_TRIVIAL(debug)
#define PINFO BOOST_LOG_TRIVIAL(info)
#define PWARNING BOOST_LOG_TRIVIAL(warning)
#define PERROR BOOST_LOG_TRIVIAL(error)
#define PFATAL BOOST_LOG_TRIVIAL(fatal)

#endif // LOG_HPP