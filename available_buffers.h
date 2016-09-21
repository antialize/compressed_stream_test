// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once

class buffer;
void create_available_buffer();
void destroy_available_buffer();

buffer * pop_available_buffer();
void push_available_buffer(buffer *);

