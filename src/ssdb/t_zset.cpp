/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <limits.h>
#include "t_zset.h"

// 分数的最大和最小范围
static const char *SSDB_SCORE_MIN		= "-9223372036854775808";
static const char *SSDB_SCORE_MAX		= "+9223372036854775807";

static int zset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &score, char log_type);
static int zdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type);
static int incr_zsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int SSDBImpl::zset(const Bytes &name, const Bytes &key, const Bytes &score, char log_type){
    // 开始事务
	Transaction trans(binlogs);

    // 先保存数据
	int ret = zset_one(this, name, key, score, log_type);
	if(ret >= 0){
		if(ret > 0){
		    // 如果新增加了数据，修改数据数量的记录
			if(incr_zsize(this, name, ret) == -1){
				return -1;
			}
		}
		// 提交事务
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("zset error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

// 删除一条记录。这些操作均大同小异
int SSDBImpl::zdel(const Bytes &name, const Bytes &key, char log_type){
	Transaction trans(binlogs);

	int ret = zdel_one(this, name, key, log_type);
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("zdel error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

// 增加value的值
int SSDBImpl::zincr(const Bytes &name, const Bytes &key, int64_t by, int64_t *new_val, char log_type){
    // 开始事务
	Transaction trans(binlogs);

	std::string old;
	int ret = this->zget(name, key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		*new_val = by;
	}else{
		*new_val = str_to_int64(old) + by;
	}

	ret = zset_one(this, name, key, str(*new_val), log_type);
	if(ret == -1){
		return -1;
	}
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = binlogs->commit();
		if(!s.ok()){
			log_error("zset error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return 1;
}

// 返回zset中内容的数量
int64_t SSDBImpl::zsize(const Bytes &name){
	std::string size_key = encode_zsize_key(name);
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

// 根据name和key获取分数
int SSDBImpl::zget(const Bytes &name, const Bytes &key, std::string *score){
	std::string buf = encode_zset_key(name, key);
	leveldb::Status s = db->Get(leveldb::ReadOptions(), buf, score);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("zget error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

// 获取迭代器，根据分数和指定的key遍历数据
static ZIterator* ziterator(
	SSDBImpl *ssdb,
	const Bytes &name, const Bytes &key_start,
	const Bytes &score_start, const Bytes &score_end,
	uint64_t limit, Iterator::Direction direction)
{
	if(direction == Iterator::FORWARD){
		std::string start, end;
		// 根据分数的key来遍历
		if(score_start.empty()){
			start = encode_zscore_key(name, key_start, SSDB_SCORE_MIN);
		}else{
			start = encode_zscore_key(name, key_start, score_start);
		}
		if(score_end.empty()){
			end = encode_zscore_key(name, "\xff", SSDB_SCORE_MAX);
		}else{
			end = encode_zscore_key(name, "\xff", score_end);
		}
		return new ZIterator(ssdb->iterator(start, end, limit), name);
	}else{
		std::string start, end;
		if(score_start.empty()){
			start = encode_zscore_key(name, key_start, SSDB_SCORE_MAX);
		}else{
			if(key_start.empty()){
				start = encode_zscore_key(name, "\xff", score_start);
			}else{
				start = encode_zscore_key(name, key_start, score_start);
			}
		}
		if(score_end.empty()){
			end = encode_zscore_key(name, "", SSDB_SCORE_MIN);
		}else{
			end = encode_zscore_key(name, "", score_end);
		}
		return new ZIterator(ssdb->rev_iterator(start, end, limit), name);
	}
}

// 获取指定的key在整个zset中的排序序号
int64_t SSDBImpl::zrank(const Bytes &name, const Bytes &key){
    // 获取迭代器，从最开始的位置开始遍历
	ZIterator *it = ziterator(this, name, "", "", "", INT_MAX, Iterator::FORWARD);
	uint64_t ret = 0;
	while(true){
		if(it->next() == false){
			ret = -1;
			break;
		}
		// 找到key
		if(key == it->key){
			break;
		}
		ret ++;
	}
	delete it;
	// 返回序号
	return ret;
}

// 获取指定key在整个zset中的反向序号
int64_t SSDBImpl::zrrank(const Bytes &name, const Bytes &key){
	ZIterator *it = ziterator(this, name, "", "", "", INT_MAX, Iterator::BACKWARD);
	uint64_t ret = 0;
	while(true){
		if(it->next() == false){
			ret = -1;
			break;
		}
		if(key == it->key){
			break;
		}
		ret ++;
	}
	delete it;
	return ret;
}

// 获取zset中从指定位置开始的迭代器
ZIterator* SSDBImpl::zrange(const Bytes &name, uint64_t offset, uint64_t limit){
	if(offset + limit > limit){
		limit = offset + limit;
	}
	ZIterator *it = ziterator(this, name, "", "", "", limit, Iterator::FORWARD);
	it->skip(offset);
	return it;
}

// 获取zset中从指定位置开始的迭代器，反向
ZIterator* SSDBImpl::zrrange(const Bytes &name, uint64_t offset, uint64_t limit){
	if(offset + limit > limit){
		limit = offset + limit;
	}
	ZIterator *it = ziterator(this, name, "", "", "", limit, Iterator::BACKWARD);
	it->skip(offset);
	return it;
}

// 遍历zset，可以指定开始的key和分数
ZIterator* SSDBImpl::zscan(const Bytes &name, const Bytes &key,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit)
{
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		this->zget(name, key, &score);
	}else{
		score = score_start.String();
	}
	return ziterator(this, name, key, score, score_end, limit, Iterator::FORWARD);
}

// 反向遍历
ZIterator* SSDBImpl::zrscan(const Bytes &name, const Bytes &key,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit)
{
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		this->zget(name, key, &score);
	}else{
		score = score_start.String();
	}
	return ziterator(this, name, key, score, score_end, limit, Iterator::BACKWARD);
}

// 从迭代器获取所有zset的name
static void get_znames(Iterator *it, std::vector<std::string> *list){
	while(it->next()){
		Bytes ks = it->key();
		//dump(ks.data(), ks.size());
		if(ks.data()[0] != DataType::ZSIZE){
			break;
		}
		std::string n;
		if(decode_zsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}
}

// 指定name的开始和结束字符串，获取zset中的所有name
int SSDBImpl::zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	start = encode_zsize_key(name_s);
	if(!name_e.empty()){
		end = encode_zsize_key(name_e);
	}
	
	Iterator *it = this->iterator(start, end, limit);
	get_znames(it, list);
	delete it;
	return 0;
}

// 获取倒序的zset中的所有name
int SSDBImpl::zrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;

	start = encode_zsize_key(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_zsize_key(name_e);
	}

	Iterator *it = this->rev_iterator(start, end, limit);
	get_znames(it, list);
	delete it;
	return 0;
}

// 确保score的值在int64的范围内？
static std::string filter_score(const Bytes &score){
	int64_t s = score.Int64();
	return str(s);
}

// returns the number of newly added items
// 添加或修改zset数据。在这里添加的时候，会向数据库中写入两条记录，一条是按name+key排序的数据，一条是按
// name+score+key排序的数据。zset大小相关的数据在这里不会保存
static int zset_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &score, char log_type){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return 0;
		//return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long!");
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long!");
		return -1;
	}
	// 确保score的值在有效分数范围之内
	std::string new_score = filter_score(score);
	std::string old_score;
	int found = ssdb->zget(name, key, &old_score);
	// 不存在或者分数不一致
	if(found == 0 || old_score != new_score){
	    // 阿，一共三个key
		std::string k0, k1, k2;

        // 更新score的值，把原来分数的操作日志删除
		if(found){
			// delete zscore key
			k1 = encode_zscore_key(name, key, old_score);
			ssdb->binlogs->Delete(k1);
		}

		// add zscore key
		// 这里会用name+score+key作为leveldb的key，value空保存一条
		// 数据，用于根据分数排序等场景
		k2 = encode_zscore_key(name, key, new_score);
		ssdb->binlogs->Put(k2, "");

		// update zset
		// 保存数据，以name+key为key来保存
		k0 = encode_zset_key(name, key);
		ssdb->binlogs->Put(k0, new_score);
		ssdb->binlogs->add_log(log_type, BinlogCommand::ZSET, k0);

		return found? 0 : 1;
	}
	return 0;
}

// 删除一条数据
static int zdel_one(SSDBImpl *ssdb, const Bytes &name, const Bytes &key, char log_type){
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long!");
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long!");
		return -1;
	}
	std::string old_score;
	int found = ssdb->zget(name, key, &old_score);
	if(found != 1){
		return 0;
	}

	std::string k0, k1;
	// 删除分数key
	// delete zscore key
	k1 = encode_zscore_key(name, key, old_score);
	ssdb->binlogs->Delete(k1);

	// delete zset
	// 删除数据记录
	k0 = encode_zset_key(name, key);
	ssdb->binlogs->Delete(k0);
	ssdb->binlogs->add_log(log_type, BinlogCommand::ZDEL, k0);

	return 1;
}

// 增加zset的项目的数量
static int incr_zsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr){
	int64_t size = ssdb->zsize(name);
	size += incr;
	// 获取到size的key
	std::string size_key = encode_zsize_key(name);
	if(size == 0){
	    // 增加计数后将数量为0，将记录删除
		ssdb->binlogs->Delete(size_key);
	}else{
	    // 保存数量
		ssdb->binlogs->Put(size_key, leveldb::Slice((char *)&size, sizeof(int64_t)));
	}
	return 0;
}
