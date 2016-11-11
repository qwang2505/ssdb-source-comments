/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_hash.h"

static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type);
static int hdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type);
static int incr_hsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
// 增加hashmap记录
int SSDBImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
    // 开始事务
	Transaction trans(binlogs);

    // 保存值
	int ret = hset_one(this, name, key, val, log_type);
	if(ret >= 0){
		if(ret > 0){
		    // 如果是新增的记录，增加尺寸
			if(incr_hsize(this, name, ret) == -1){
				return -1;
			}
		}
		// 提交事务
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

// 删除记录
int SSDBImpl::hdel(const Bytes &name, const Bytes &key, char log_type){
	Transaction trans(binlogs);

	int ret = hdel_one(this, name, key, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

// 增加记录
int SSDBImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
	Transaction trans(binlogs);

	std::string old;
	// 先获取
	int ret = this->hget(name, key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
	    // 增加值
		*new_val = str_to_int64(old) + by;
		if(errno != 0){
			return 0;
		}
	}

    // 保存记录
	ret = hset_one(this, name, key, str(*new_val), log_type);
	if(ret == -1){
		return -1;
	}
	if(ret >= 0){
		if(ret > 0){
		    // 如果是新增的记录，增加尺寸
			if(incr_hsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return 1;
}

// 返回hashmap的大小，也就是field的数量
// 直接从leveldb中获取即可
int64_t SSDBImpl::hsize(const Bytes &name){
	std::string size_key = encode_hsize_key(name);
	std::string val;
	leveldb::Status s;

	s = db->Get(leveldb::ReadOptions(), size_key, &val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		return -1;
	}else{
		if(val.size() != sizeof(uint64_t)){
			return 0;
		}
		int64_t ret = *(int64_t *)val.data();
		return ret < 0? 0 : ret;
	}
}

// 清空hashmap
int64_t SSDBImpl::hclear(const Bytes &name){
	int64_t count = 0;
	while(1){
	    // 使用迭代器遍历hashmap，每次处理1000条记录
		HIterator *it = this->hscan(name, "", "", 1000);
		int num = 0;
		// 依次删除每条hashmap记录
		while(it->next()){
			int ret = this->hdel(name, it->key);
			if(ret == -1){
				delete it;
				return 0;
			}
			num ++;
		};
		// 用完迭代器别忘记释放内存
		delete it;

        // 如果没有更多了，退出
		if(num == 0){
			break;
		}
		// 记录数量
		count += num;
	}
	return count;
}

// 根据name和key获取数据值
int SSDBImpl::hget(const Bytes &name, const Bytes &key, std::string *val){
    // 将name和key编码，作为leveldb的key
	std::string dbkey = encode_hash_key(name, key);
	leveldb::Status s = db->Get(leveldb::ReadOptions(), dbkey, val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		return -1;
	}
	return 1;
}

// 遍历hashmap，返回迭代器用于遍历
HIterator* SSDBImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

    // 获取到开始和结束的key的范围
	key_start = encode_hash_key(name, start);
	if(!end.empty()){
		key_end = encode_hash_key(name, end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

    // 创建迭代器
	return new HIterator(this->iterator(key_start, key_end, limit), name);
}

// 反向遍历
HIterator* SSDBImpl::hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_hash_key(name, start);
	if(start.empty()){
	    // TODO 这是什么？
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = encode_hash_key(name, end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new HIterator(this->rev_iterator(key_start, key_end, limit), name);
}

// 根据迭代器，获取hashmap所有的name
static void get_hnames(Iterator *it, std::vector<std::string> *list){
	while(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] != DataType::HSIZE){
			break;
		}
		std::string n;
		if(decode_hsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}
}

// 根据指定的name区间，遍历得到所有的names
int SSDBImpl::hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	start = encode_hsize_key(name_s);
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}
	
	Iterator *it = this->iterator(start, end, limit);
	get_hnames(it, list);
	delete it;
	return 0;
}

// 反向遍历所有的names
int SSDBImpl::hrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	start = encode_hsize_key(name_s);
	if(name_s.empty()){
	    // TODO 为啥哪都有你？
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}
	
	Iterator *it = this->rev_iterator(start, end, limit);
	get_hnames(it, list);
	delete it;
	return 0;
}

// returns the number of newly added items
// 在这个函数中的操作是通过binlog来实现的，外部应该会有事务的处理
// 设置一个hashmap的值，在这里只做数据的处理
static int hset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val, char log_type){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	int ret = 0;
	std::string dbval;
	// 根据name和key获取数据
	if(ssdb->hget(name, key, &dbval) == 0){ // not found
	    // 不存在，是新增的记录
		std::string hkey = encode_hash_key(name, key);
		// 添加记录到binlog
		ssdb->binlogs->Put(hkey, slice(val));
		ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		ret = 1;
	}else{
	    // 新值和旧值不同，更新
		if(dbval != val){
			std::string hkey = encode_hash_key(name, key);
			ssdb->binlogs->Put(hkey, slice(val));
			ssdb->binlogs->add_log(log_type, BinlogCommand::HSET, hkey);
		}
		// 如果相同，不需要进行操作
		ret = 0;
	}
	return ret;
}

// 删除一个记录
static int hdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type){
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	std::string dbval;
	if(ssdb->hget(name, key, &dbval) == 0){
		return 0;
	}

	std::string hkey = encode_hash_key(name, key);
	ssdb->binlogs->Delete(hkey);
	ssdb->binlogs->add_log(log_type, BinlogCommand::HDEL, hkey);
	
	return 1;
}

// 增加指定name的hashmap的记录的数量
static int incr_hsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr){
	int64_t size = ssdb->hsize(name);
	size += incr;
	std::string size_key = encode_hsize_key(name);
	if(size == 0){
		ssdb->binlogs->Delete(size_key);
	}else{
		ssdb->binlogs->Put(size_key, leveldb::Slice((char *)&size, sizeof(int64_t)));
	}
	return 0;
}
