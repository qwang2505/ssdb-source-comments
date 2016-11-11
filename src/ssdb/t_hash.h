/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_HASH_H_
#define SSDB_HASH_H_

#include "ssdb_impl.h"

/*
 * hashmap的数据是如下结构的：
 * name1: 
 *   field1: value1
 *   field2: value2
 * name2:
 *   field1: value1
 *   field2: value2
 *   ...
 *
 * 在SSDB中存储hashmap的时候，主要包括两部分：
 * 1. 存储hashmap的数据，在leveldb中，以name+field作为leveldb的key，value作为leveldb的value存储；
 * 2. 存储hashmap的大小，以name作为hashmap的key，value是hashmap中field的数量。
 *
 * 下面的四个函数，分别用于编码和解码上述两种存储方式的key
 * 因此，hsize是个O(1)的操作。
 */

inline static
std::string encode_hsize_key(const Bytes &name){
	std::string buf;
	buf.append(1, DataType::HSIZE);
	buf.append(name.data(), name.size());
	return buf;
}

inline static
int decode_hsize_key(const Bytes &slice, std::string *name){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(name) == -1){
		return -1;
	}
	return 0;
}

inline static
std::string encode_hash_key(const Bytes &name, const Bytes &key){
	std::string buf;
	buf.append(1, DataType::HASH);
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	buf.append(1, '=');
	buf.append(key.data(), key.size());
	return buf;
}

inline static
int decode_hash_key(const Bytes &slice, std::string *name, std::string *key){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_8_data(name) == -1){
		return -1;
	}
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(key) == -1){
		return -1;
	}
	return 0;
}

#endif
