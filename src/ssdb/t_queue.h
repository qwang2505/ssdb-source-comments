/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_QUEUE_H_
#define SSDB_QUEUE_H_

#include "ssdb_impl.h"

// 下面指定了队列中元素的序号定义，头部序号的序号是2，这个序号
// 记录的不是真正的数据，而是队列头部的真正序号。
// 同样尾部序号也是一样的
// 真正队列中元素的序号从10000开始

const uint64_t QFRONT_SEQ = 2;
const uint64_t QBACK_SEQ  = 3;
const uint64_t QITEM_MIN_SEQ = 10000;
const uint64_t QITEM_MAX_SEQ = 9223372036854775807ULL;
// 初始化队列的时候，从队列序号的中间开始设置序号，以保证能从队头和
// 队尾向队列中添加元素
const uint64_t QITEM_SEQ_INIT = QITEM_MAX_SEQ/2;

/**
 * 队列在ssdb中的存储需要保存两部分数据：
 * 1. 保存队列中内容的数据，以队列的name加上项在队列中的序号作为leveldb的key，value作为leveldb的value；
 * 2. 保存队列的长度，name作为key，长度作为value
 */

inline static
std::string encode_qsize_key(const Bytes &name){
	std::string buf;
	buf.append(1, DataType::QSIZE);
	buf.append(name.data(), name.size());
	return buf;
}

inline static
int decode_qsize_key(const Bytes &slice, std::string *name){
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
std::string encode_qitem_key(const Bytes &name, uint64_t seq){
	std::string buf;
	buf.append(1, DataType::QUEUE);
	buf.append(1, (uint8_t)name.size());
	buf.append(name.data(), name.size());
	seq = big_endian(seq);
	buf.append((char *)&seq, sizeof(uint64_t));
	return buf;
}

inline static
int decode_qitem_key(const Bytes &slice, std::string *name, uint64_t *seq){
	Decoder decoder(slice.data(), slice.size());
	if(decoder.skip(1) == -1){
		return -1;
	}
	// 这里如果name是NULL，则不读取，指针还是会向前，不影响读数据
	if(decoder.read_8_data(name) == -1){
		return -1;
	}
	if(decoder.read_uint64(seq) == -1){
		return -1;
	}
	*seq = big_endian(*seq);
	return 0;
}

#endif
