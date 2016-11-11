/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_kv.h"

// 下面貌似所有写操作都用到了事务，而事务会加锁，不过这也是没办法的事情吧

// 批量添加或修改KV数据，使用事务来实现一次性写入多条的操作
// offset参数听由意思的，但是真的有用吗？
int SSDBImpl::multi_set(const std::vector<Bytes> &kvs, int offset, char log_type){
    // 开始事务，这里会加锁，使用leveldb的batch实现批量操作
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		if(key.empty()){
			log_error("empty key!");
			return 0;
			//return -1;
		}
		const Bytes &val = *(it + 1);
		std::string buf = encode_kv_key(key);
		// 添加KV数据
		binlogs->Put(buf, slice(val));
		// 添加操作日志
		binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	}
	// 提交事务，这里会真正执行操作
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

// 批量删除多条数据，事务操作，类似
int SSDBImpl::multi_del(const std::vector<Bytes> &keys, int offset, char log_type){
    // 开始事务
	Transaction trans(binlogs);

	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
		std::string buf = encode_kv_key(key);
		binlogs->Delete(buf);
		binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	}
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("multi_del error: %s", s.ToString().c_str());
		return -1;
	}
	return keys.size() - offset;
}

// 单次操作也要加事务？应该是为了保证操作日志吧，为了保证主从同步？
int SSDBImpl::set(const Bytes &key, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(binlogs);

	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, slice(val));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::setnx(const Bytes &key, const Bytes &val, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	// 开始事务
	Transaction trans(binlogs);

	std::string tmp;
	int found = this->get(key, &tmp);
	// key已经存在，不进行操作。此时相当于事务没有执行，在下次创建事务的时候会把
	// 当前的操作删除掉，相当于操作没有发生。实际上这里上面也没有别的操作
	if(found != 0){
		return 0;
	}
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, slice(val));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int SSDBImpl::getset(const Bytes &key, std::string *val, const Bytes &newval, char log_type){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	// 开始事务
	Transaction trans(binlogs);

	int found = this->get(key, val);
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, slice(newval));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return found;
}

// 删除数据
int SSDBImpl::del(const Bytes &key, char log_type){
	Transaction trans(binlogs);

	std::string buf = encode_kv_key(key);
	binlogs->begin();
	binlogs->Delete(buf);
	binlogs->add_log(log_type, BinlogCommand::KDEL, buf);
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

// 增加数据的值
int SSDBImpl::incr(const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	Transaction trans(binlogs);

	std::string old;
	int ret = this->get(key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
		*new_val = str_to_int64(old) + by;
		if(errno != 0){
			return 0;
		}
	}

	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, str(*new_val));
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);

	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

// 获取数据，这就不需要事务了
int SSDBImpl::get(const Bytes &key, std::string *val){
	std::string buf = encode_kv_key(key);

	leveldb::Status s = db->Get(leveldb::ReadOptions(), buf, val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

// 遍历指定区间的数据，返回一个KV的迭代器
KIterator* SSDBImpl::scan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;
	key_start = encode_kv_key(start);
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->iterator(key_start, key_end, limit));
}

// 反向遍历
KIterator* SSDBImpl::rscan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_kv_key(start);
	if(start.empty()){
	    // 这是什么意思
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->rev_iterator(key_start, key_end, limit));
}

// 设置value的比特位的值
int SSDBImpl::setbit(const Bytes &key, int bitoffset, int on, char log_type){
	if(key.empty()){
		log_error("empty key!");
		return 0;
	}
	Transaction trans(binlogs);
	
	std::string val;
	int ret = this->get(key, &val);
	if(ret == -1){
		return -1;
	}
	
	// 一共几个字节
	int len = bitoffset / 8;
	// 字节里的第几个bit
	int bit = bitoffset % 8;
	if(len >= val.size()){
		val.resize(len + 1, 0);
	}
	// 原来的值
	int orig = val[len] & (1 << bit);
	if(on == 1){
	    // 将对应为设置为1
		val[len] |= (1 << bit);
	}else{
	    // 将对应位设置为0
		val[len] &= ~(1 << bit);
	}

    // 更新值
	std::string buf = encode_kv_key(key);
	binlogs->Put(buf, val);
	binlogs->add_log(log_type, BinlogCommand::KSET, buf);
	// 提交事务
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	// 返回原始的值
	return orig;
}

// 获取bit的值
int SSDBImpl::getbit(const Bytes &key, int bitoffset){
	std::string val;
	int ret = this->get(key, &val);
	if(ret == -1){
		return -1;
	}
	
	int len = bitoffset / 8;
	int bit = bitoffset % 8;
	// 长度不够，返回0
	if(len >= val.size()){
		return 0;
	}
	// 返回对应位的值
	return val[len] & (1 << bit);
}


