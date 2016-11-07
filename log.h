// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once
#include <mutex>
#include <iostream>

struct crapper {
	static std::mutex m;
	std::unique_lock<std::mutex> l;
	crapper(): l(m) {}

	const crapper & operator <<(std::ostream & (*f)(std::ostream &)) const {
		std::cout << f;
		return *this;
	}
};


template <typename T>
const crapper & operator <<(const crapper & c, const T & t) {
	std::cout << t;
	return c;
}

inline crapper log_info() {
	return crapper();
}
