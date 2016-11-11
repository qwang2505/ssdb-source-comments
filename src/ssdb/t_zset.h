/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_ZSET_H_
#define SSDB_ZSET_H_

#include "ssdb_impl.h"

#define encode_score(s) big_endian((uint64_t)(s))
#define decode_score(s) big_endian((uint64_t)(s))

/**
 * 为了支持zset的实现，在leveldb中共需要保存三种类型的数据：
 * 1. 以name+key作为leveldb的key，score作为leveldb的value；
 * 2. 以name+score+key作为leveldb的key，leveldb的value为空。存储这个数据是为了方便的对zset进行排序相关操作。
 * 3. 以name作为leveldb的key，leveldb的value存储此zset的大小
 */

static inline
std::string encode_zsize_key(const Bytes &name){
	std::string buf;
	buf.append(1, DataType::ZSIZE);
	buf.append(name.data(), name.size());
	return buf;
}

inline static
int decode_zsize_key(const Bytes &slice, std::string *name){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_data(name) == -1){
		return -1;
	}
	return 0;
}

static inline
std::string encode_zset_key(const Bytes &name, const Bytes &key){
	std::string buf;
	buf.append(1, DataType::ZSET);
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());
	return buf;
}

static inline
int decode_zset_key(const Bytes &slice, std::string *name, std::string *key){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	if(decoder.read_8_data(name) == -1){
		return -1;
	}
	if(decoder.read_8_data(key) == -1){
		return -1;
	}
	return 0;
}

// type, len, key, score, =, val
static inline
std::string encode_zscore_key(const Bytes &key, const Bytes &val, const Bytes &score){
	std::string buf;
	buf.append(1, DataType::ZSCORE);
	buf.append(1, (uint8_t)key.size());
	buf.append(key.data(), key.size());

	int64_t s = score.Int64();
	if(s < 0){
		buf.append(1, '-');
	}else{
		buf.append(1, '=');
	}
	s = encode_score(s);

	buf.append((char *)&s, sizeof(int64_t));
	buf.append(1, '=');
	buf.append(val.data(), val.size());
	return buf;
}

static inline
int decode_zscore_key(const Bytes &slice, std::string *name, std::string *key, std::string *score){
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
	int64_t s;
	if(decoder.read_int64(&s) == -1){
		return -1;
	}else{
		if(score != NULL){
			s = decode_score(s);
			score->assign(str(s));
		}
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
