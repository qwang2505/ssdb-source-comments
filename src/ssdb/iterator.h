/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include "../util/bytes.h"

// 生命leveldb的迭代器，在下面的定义中会用到
namespace leveldb{
	class Iterator;
}

// ssdb自己的迭代器，对leveldb的迭代器进行封装，完成skip/limit等功能
// 这个是最普通的迭代器，可以指向任何数据
class Iterator{
public:
    // 迭代方向的每句变量
	enum Direction{
		FORWARD, BACKWARD
	};
	// 创建迭代器，默认方向是向前
	Iterator(leveldb::Iterator *it,
			const std::string &end,
			uint64_t limit,
			Direction direction=Iterator::FORWARD);
	~Iterator();
	bool skip(uint64_t offset);
	bool next();
	Bytes key();
	Bytes val();
private:
	leveldb::Iterator *it;
	std::string end;
	uint64_t limit;
	bool is_first;
	int direction;
};

// 下面几个针对几种数据类型的迭代器均在上面的基本迭代器的基础上实现，
// 同时根据需求实现一些额外的功能，比如：
// 1. 获取真实的key。存储到leveldb里的key添加了数据类型标示，在将key返回给用户的时候
//  需要先解码出真实的key；
// 2. 非KV的数据类型，获取到真实的数据。比如hashset，zset等。

// key/value数据的迭代器
class KIterator{
public:
    // 当前数据的key
	std::string key;
	// 当前数据的value
	std::string val;

    // 创建KV迭代器
	KIterator(Iterator *it);
	~KIterator();
	// 设置是否将值存储到迭代器中
	void return_val(bool onoff);
	// 移动到下一个
	bool next();
private:
	Iterator *it;
	bool return_val_;
};

// hashset的迭代器
class HIterator{
public:
	std::string name;
	std::string key;
	std::string val;

	HIterator(Iterator *it, const Bytes &name);
	~HIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


// sorted set的迭代器
class ZIterator{
public:
	std::string name;
	std::string key;
	std::string score;

	ZIterator(Iterator *it, const Bytes &name);
	~ZIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};


#endif
