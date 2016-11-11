/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_KV_H_
#define SSDB_KV_H_

#include "ssdb_impl.h"

// 编码和解码KV数据结构在leveldb中存储的key
// 在leveldb中存储KV数据的时候，在数据的key的前面加了一个k，标示
// 数据是KV类型的
//
// SSDB中KV的数据直接存储在leveldb中，只是在key的前面加了k标示数据类型，
// 没有其他区别了

static inline
std::string encode_kv_key(const Bytes &key){
	std::string buf;
	buf.append(1, DataType::KV);
	buf.append(key.data(), key.size());
	return buf;
}

static inline
int decode_kv_key(const Bytes &slice, std::string *key){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(key) == -1){
		return -1;
	}
	return 0;
}

#endif
