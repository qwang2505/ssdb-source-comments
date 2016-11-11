/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "iterator.h"
#include "t_kv.h"
#include "t_hash.h"
#include "t_zset.h"
#include "t_queue.h"
#include "../util/log.h"
#include "../util/config.h"
#include "leveldb/iterator.h"

// 初始化迭代器，传入leveldb的迭代器，将其封装成ssdb自己的迭代器
Iterator::Iterator(leveldb::Iterator *it,
		const std::string &end,
		uint64_t limit,
		Direction direction)
{
	this->it = it;
	this->end = end;
	this->limit = limit;
	this->is_first = true;
	this->direction = direction;
}

// 删除迭代器，将leveldb的迭代器删除
Iterator::~Iterator(){
	delete it;
}

// 返回迭代器指向的当前元素的key
Bytes Iterator::key(){
	leveldb::Slice s = it->key();
	return Bytes(s.data(), s.size());
}

// 返回迭代器指向的当前元素的value
Bytes Iterator::val(){
	leveldb::Slice s = it->value();
	return Bytes(s.data(), s.size());
}

// 实现skip功能。如果超出迭代器范围，返回false，否则返回true
bool Iterator::skip(uint64_t offset){
	while(offset-- > 0){
		if(this->next() == false){
			return false;
		}
	}
	return true;
}

// 把迭代器向后移动，指向下一个元素。如果返回false，说明迭代器已经完了，或者超出limit
// 貌似leveldb的迭代器在创建后直接指向了第一个元素，而这里封装之后则将其统一，要返回第
// 一个元素也需要先调用next方法
bool Iterator::next(){
    // 超出limit
	if(limit == 0){
		return false;
	}
	// 如果是第一个，不需要移动
	if(is_first){
		is_first = false;
	}else{
	    // 否则，移动迭代器
		if(direction == FORWARD){
			it->Next();
		}else{
			it->Prev();
		}
	}

    // 没有下一个元素了，返回false
	if(!it->Valid()){
		// make next() safe to be called after previous return false.
		limit = 0;
		return false;
	}
	// 判断是否已经到了end
	if(direction == FORWARD){
		if(!end.empty() && it->key().compare(end) > 0){
			limit = 0;
			return false;
		}
	}else{
		if(!end.empty() && it->key().compare(end) < 0){
			limit = 0;
			return false;
		}
	}
	// 减少limit
	limit --;
	return true;
}


/* KV */

KIterator::KIterator(Iterator *it){
	this->it = it;
	// 默认保存值
	this->return_val_ = true;
}

KIterator::~KIterator(){
	delete it;
}

void KIterator::return_val(bool onoff){
	this->return_val_ = onoff;
}

bool KIterator::next(){
    // 调用基本迭代器的next方法移动迭代器
	while(it->next()){
	    // 获取key value
		Bytes ks = it->key();
		Bytes vs = it->val();
		//dump(ks.data(), ks.size(), "z.next");
		//dump(vs.data(), vs.size(), "z.next");
		// 判断数据类型是否KV，如果不是，返回错误
		if(ks.data()[0] != DataType::KV){
			return false;
		}
		// 解码出真实的key，存储到leveldb中的key添加了数据类型来区分数据类型
		if(decode_kv_key(ks, &this->key) == -1){
			continue;
		}
		if(return_val_){
		    // 保存数据
			this->val.assign(vs.data(), vs.size());
		}
		return true;
	}
	return  false;
}

/* HASH */

// hashset的迭代器，同样也是使用基本迭代器来实现
HIterator::HIterator(Iterator *it, const Bytes &name){
	this->it = it;
	// hashset的名称
	this->name.assign(name.data(), name.size());
	this->return_val_ = true;
}

HIterator::~HIterator(){
	delete it;
}

void HIterator::return_val(bool onoff){
	this->return_val_ = onoff;
}

bool HIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		Bytes vs = it->val();
		//dump(ks.data(), ks.size(), "z.next");
		//dump(vs.data(), vs.size(), "z.next");
		// 验证数据类型
		if(ks.data()[0] != DataType::HASH){
			return false;
		}
		std::string n;
		// 解析出hashset的key和name
		if(decode_hash_key(ks, &n, &key) == -1){
			continue;
		}
		// 如果name不一样，标示当前name的hashset已经迭代完了，已经到下一个数据了
		if(n != this->name){
			return false;
		}
		if(return_val_){
		    // 保存value
			this->val.assign(vs.data(), vs.size());
		}
		return true;
	}
	return false;
}

/* ZSET */

ZIterator::ZIterator(Iterator *it, const Bytes &name){
	this->it = it;
	// 指定zset的name
	this->name.assign(name.data(), name.size());
}

ZIterator::~ZIterator(){
	delete it;
}
		
// 实现skip功能
bool ZIterator::skip(uint64_t offset){
	while(offset-- > 0){
		if(this->next() == false){
			return false;
		}
	}
	return true;
}

bool ZIterator::next(){
	while(it->next()){
		Bytes ks = it->key();
		//Bytes vs = it->val();
		//dump(ks.data(), ks.size(), "z.next");
		//dump(vs.data(), vs.size(), "z.next");
		// 验证数据类型
		if(ks.data()[0] != DataType::ZSCORE){
			return false;
		}
		// 从key中解析key和score
		if(decode_zscore_key(ks, NULL, &key, &score) == -1){
			continue;
		}
		return true;
	}
	return false;
}
