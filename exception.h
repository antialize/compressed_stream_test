#pragma once

#include <exception>

struct exception : public std::runtime_error {
	exception(const std::string & s) : std::runtime_error(s) {}
};