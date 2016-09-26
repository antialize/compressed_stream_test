// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; eval: (progn (c-set-style "stroustrup") (c-set-offset 'innamespace 0)); -*-
// vi:set ts=4 sts=4 sw=4 noet :
#pragma once

class block;
void create_available_block();
void destroy_available_block();

block * pop_available_block();
void push_available_block(block *);

