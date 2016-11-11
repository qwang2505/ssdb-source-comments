/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_OPTION_H_
#define SSDB_OPTION_H_

#include "../util/config.h"

// 比较简单，就是记录一些选项。通过Config对象来进行初始化。
// 记录的是leveldb相关的选项，在开启leveldb的时候会用到
class Options
{
public:
	Options();
	~Options(){}
	
	void load(const Config &conf);

	size_t cache_size;
	size_t max_open_files;
	size_t write_buffer_size;
	size_t block_size;
	int compaction_speed;
	std::string compression;
	bool binlog;
};

#endif
