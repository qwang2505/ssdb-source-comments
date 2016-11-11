/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "t_queue.h"

// 下面的写操作均是通过事务来实现的

// 根据name和序号获取value
static int qget_by_seq(leveldb::DB* db, const Bytes &name, uint64_t seq, std::string *val){
	std::string key = encode_qitem_key(name, seq);
	leveldb::Status s;

	s = db->Get(leveldb::ReadOptions(), key, val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("Get() error!");
		return -1;
	}else{
		return 1;
	}
}

// 也是从队列中获取数据，只是获取的数据是int
static int qget_uint64(leveldb::DB* db, const Bytes &name, uint64_t seq, uint64_t *ret){
	std::string val;
	*ret = 0;
	int s = qget_by_seq(db, name, seq, &val);
	if(s == 1){
		if(val.size() != sizeof(uint64_t)){
			return -1;
		}
		*ret = *(uint64_t *)val.data();
	}
	return s;
}

// 根据name和序号，从队列中删除一个元素
static int qdel_one(SSDBImpl *ssdb, const Bytes &name, uint64_t seq){
	std::string key = encode_qitem_key(name, seq);
	leveldb::Status s;

	ssdb->binlogs->Delete(key);
	return 0;
}

// 向队列中添加一个元素
static int qset_one(SSDBImpl *ssdb, const Bytes &name, uint64_t seq, const Bytes &item){
	std::string key = encode_qitem_key(name, seq);
	leveldb::Status s;

	ssdb->binlogs->Put(key, slice(item));
	return 0;
}

// 增加队列的长度
static int64_t incr_qsize(SSDBImpl *ssdb, const Bytes &name, int64_t incr){
	int64_t size = ssdb->qsize(name);
	if(size == -1){
		return -1;
	}
	size += incr;
	if(size <= 0){
	    // 修改后队列为空，将相关信息均删除
		ssdb->binlogs->Delete(encode_qsize_key(name));
		// 删除队头和队尾的特殊标识？
		qdel_one(ssdb, name, QFRONT_SEQ);
		qdel_one(ssdb, name, QBACK_SEQ);
	}else{
	    // 修改队列长度的记录
		ssdb->binlogs->Put(encode_qsize_key(name), leveldb::Slice((char *)&size, sizeof(size)));
	}
	return size;
}

/****************/

// 根据name获取队列的长度
int64_t SSDBImpl::qsize(const Bytes &name){
	std::string key = encode_qsize_key(name);
	std::string val;

	leveldb::Status s;
	s = db->Get(leveldb::ReadOptions(), key, &val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		log_error("Get() error!");
		return -1;
	}else{
		if(val.size() != sizeof(uint64_t)){
			return -1;
		}
		return *(int64_t *)val.data();
	}
}

// @return 0: empty queue, 1: item peeked, -1: error
// 返回队列头部的元素
int SSDBImpl::qfront(const Bytes &name, std::string *item){
	int ret = 0;
	uint64_t seq;
	// 先获取到队列头部元素的序号，存放在特定的记录name+QFRONT_SEQ中
	ret = qget_uint64(this->db, name, QFRONT_SEQ, &seq);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	// 根据头部元素的序号获取值
	ret = qget_by_seq(this->db, name, seq, item);
	return ret;
}

// @return 0: empty queue, 1: item peeked, -1: error
// 返回队列尾部的元素
int SSDBImpl::qback(const Bytes &name, std::string *item){
	int ret = 0;
	uint64_t seq;
	// 获取队列尾部元素的序号
	ret = qget_uint64(this->db, name, QBACK_SEQ, &seq);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	// 根据序号获取队列尾部的元素
	ret = qget_by_seq(this->db, name, seq, item);
	return ret;
}

// 根据序号设置队列中的元素
int SSDBImpl::qset_by_seq(const Bytes &name, uint64_t seq, const Bytes &item, char log_type){
    // 开始事务
	Transaction trans(binlogs);
	// 记录最大和最小序号
	uint64_t min_seq, max_seq;
	int ret;
	// 获取队列大小
	int64_t size = this->qsize(name);
	if(size == -1){
		return -1;
	}
	// 获取头部元素的序号
	ret = qget_uint64(this->db, name, QFRONT_SEQ, &min_seq);
	if(ret == -1){
		return -1;
	}
	// 计算得到尾部元素的序号
	max_seq = min_seq + size;
	// 序号超出范围，返回错误
	if(seq < min_seq || seq > max_seq){
		return 0;
	}

    // 根据序号设置值
	ret = qset_one(this, name, seq, item);
	if(ret == -1){
		return -1;
	}

    // 添加操作日志
	std::string buf = encode_qitem_key(name, seq);
	binlogs->add_log(log_type, BinlogCommand::QSET, buf);

    // 提交事务
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 1;
}

// return: 0: index out of range, -1: error, 1: ok
// 根据索引值设置队列元素的值
int SSDBImpl::qset(const Bytes &name, int64_t index, const Bytes &item, char log_type){
    // 开始事务
	Transaction trans(binlogs);
	// 获取队列大小
	int64_t size = this->qsize(name);
	if(size == -1){
		return -1;
	}
	// 超出索引范围
	if(index >= size || index < -size){
		return 0;
	}
	
	int ret;
	uint64_t seq;
	if(index >= 0){
	    // 从头部开始的索引值，先获取头部元素的序号
		ret = qget_uint64(this->db, name, QFRONT_SEQ, &seq);
		// 计算获取指定索引的元素的序号
		seq += index;
	}else{
	    // 从尾部开始的索引值，县获取尾部元素的序号
		ret = qget_uint64(this->db, name, QBACK_SEQ, &seq);
		// 计算得到指定索引的元素的序号
		seq += index + 1;
	}
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

    // 根据序号设置元素值
	ret = qset_one(this, name, seq, item);
	if(ret == -1){
		return -1;
	}

	//log_info("qset %s %" PRIu64 "", hexmem(name.data(), name.size()).c_str(), seq);
	// 添加操作日志
	std::string buf = encode_qitem_key(name, seq);
	binlogs->add_log(log_type, BinlogCommand::QSET, buf);
	
	// 提交事务
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 1;
}

// 向队列push一个元素，指定push到队头还是队尾
int64_t SSDBImpl::_qpush(const Bytes &name, const Bytes &item, uint64_t front_or_back_seq, char log_type){
    // 开始事务
	Transaction trans(binlogs);

	int ret;
	// generate seq
	uint64_t seq;
	// 获取队头或者队尾的元素的序号
	ret = qget_uint64(this->db, name, front_or_back_seq, &seq);
	if(ret == -1){
		return -1;
	}
	// update front and/or back
	if(ret == 0){
	    // 返回值是0，表示队列是空的
	    // 设置队列的初始化序号，设置到序号区间的中间位置，以保证可以从头部和尾部push元素
		seq = QITEM_SEQ_INIT;
		// 设置队列头部和队列尾部的元素的序号值
		ret = qset_one(this, name, QFRONT_SEQ, Bytes(&seq, sizeof(seq)));
		if(ret == -1){
			return -1;
		}
		ret = qset_one(this, name, QBACK_SEQ, Bytes(&seq, sizeof(seq)));
	}else{
	    // 队列非空，通过老的序号得到新的序号值
		seq += (front_or_back_seq == QFRONT_SEQ)? -1 : +1;
		// 设置队列头部或者尾部的序号值
		ret = qset_one(this, name, front_or_back_seq, Bytes(&seq, sizeof(seq)));
	}
	if(ret == -1){
		return -1;
	}
	// 超出队列大小的范围，返回错误
	if(seq <= QITEM_MIN_SEQ || seq >= QITEM_MAX_SEQ){
		log_info("queue is full, seq: %" PRIu64 " out of range", seq);
		return -1;
	}
	
	// prepend/append item
	// 设置对应序号的队列元素的值
	ret = qset_one(this, name, seq, item);
	if(ret == -1){
		return -1;
	}

    // 添加操作日志
	std::string buf = encode_qitem_key(name, seq);
	if(front_or_back_seq == QFRONT_SEQ){
		binlogs->add_log(log_type, BinlogCommand::QPUSH_FRONT, buf);
	}else{
		binlogs->add_log(log_type, BinlogCommand::QPUSH_BACK, buf);
	}
	
	// update size
	// 增加队列长度
	int64_t size = incr_qsize(this, name, +1);
	if(size == -1){
		return -1;
	}

    // 提交事务，一切OK
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return size;
}

// 向队列头部push元素
int64_t SSDBImpl::qpush_front(const Bytes &name, const Bytes &item, char log_type){
	return _qpush(name, item, QFRONT_SEQ, log_type);
}

// 向队列尾部push元素
int64_t SSDBImpl::qpush_back(const Bytes &name, const Bytes &item, char log_type){
	return _qpush(name, item, QBACK_SEQ, log_type);
}

// 从队列头部或者尾部弹出一个元素
int SSDBImpl::_qpop(const Bytes &name, std::string *item, uint64_t front_or_back_seq, char log_type){
    // 开始事务
	Transaction trans(binlogs);
	
	int ret;
	uint64_t seq;
	// 获取头部或尾部元素的序号值
	ret = qget_uint64(this->db, name, front_or_back_seq, &seq);
	if(ret == -1){
		return -1;
	}
	// 空队列
	if(ret == 0){
		return 0;
	}
	
	// 根据序号获取元素值
	ret = qget_by_seq(this->db, name, seq, item);
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}

	// delete item
	// 根据序号删除元素值
	ret = qdel_one(this, name, seq);
	if(ret == -1){
		return -1;
	}

    // 添加操作日志
	if(front_or_back_seq == QFRONT_SEQ){
		binlogs->add_log(log_type, BinlogCommand::QPOP_FRONT, name.String());
	}else{
		binlogs->add_log(log_type, BinlogCommand::QPOP_BACK, name.String());
	}

	// update size
	// 修改队列长度
	int64_t size = incr_qsize(this, name, -1);
	if(size == -1){
		return -1;
	}
		
	// update front
	if(size > 0){
		seq += (front_or_back_seq == QFRONT_SEQ)? +1 : -1;
		//log_debug("seq: %" PRIu64 ", ret: %d", seq, ret);
		// 弹出元素后，更新队列头部或尾部的元素的序号
		ret = qset_one(this, name, front_or_back_seq, Bytes(&seq, sizeof(seq)));
		if(ret == -1){
			return -1;
		}
	}

	// 提交所有事务
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 1;
}

// @return 0: empty queue, 1: item popped, -1: error
// 从头部弹出一个元素
int SSDBImpl::qpop_front(const Bytes &name, std::string *item, char log_type){
	return _qpop(name, item, QFRONT_SEQ, log_type);
}

// 从尾部弹出一个元素
int SSDBImpl::qpop_back(const Bytes &name, std::string *item, char log_type){
	return _qpop(name, item, QBACK_SEQ, log_type);
}

// 根据迭代器获取所有队列的名称
static void get_qnames(Iterator *it, std::vector<std::string> *list){
	while(it->next()){
		Bytes ks = it->key();
		//dump(ks.data(), ks.size());
		if(ks.data()[0] != DataType::QSIZE){
			break;
		}
		std::string n;
		if(decode_qsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}
}

// 指定队列名称开始和结束的位置，获取所有队列的名称
int SSDBImpl::qlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	// 编码获取到开始和结束的name
	start = encode_qsize_key(name_s);
	if(!name_e.empty()){
		end = encode_qsize_key(name_e);
	}
	
	// 获取基础遍历器
	Iterator *it = this->iterator(start, end, limit);
	// 获取所有名称
	get_qnames(it, list);
	delete it;
	return 0;
}

// 倒序获取所有队列名称，指定开始和结束的name
int SSDBImpl::qrlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	
	start = encode_qsize_key(name_s);
	if(name_s.empty()){
		start.append(1, 255);
	}
	if(!name_e.empty()){
		end = encode_qsize_key(name_e);
	}
	
	Iterator *it = this->rev_iterator(start, end, limit);
	get_qnames(it, list);
	delete it;
	return 0;
}

// 修复队列信息，通过遍历整个队列，修复队列长度、队头/队尾序号等信息
int SSDBImpl::qfix(const Bytes &name){
    // 开始事务
	Transaction trans(binlogs);
	// 开始和结束的key是最小和最大的元素序号值
	std::string key_s = encode_qitem_key(name, QITEM_MIN_SEQ - 1);
	std::string key_e = encode_qitem_key(name, QITEM_MAX_SEQ);

	bool error = false;
	uint64_t seq_min = 0;
	uint64_t seq_max = 0;
	uint64_t count = 0;
	// 获取迭代器，遍历整个队列
	Iterator *it = this->iterator(key_s, key_e, QITEM_MAX_SEQ);
	// 遍历获取队列最大和最小序号。如果出现错误数据，返回错误信息
	while(it->next()){
		//dump(it->key().data(), it->key().size());
		if(seq_min == 0){
		    // 初始化最小序号
		    // 这里name为NULL，表示不读取name，不影响解码
			if(decode_qitem_key(it->key(), NULL, &seq_min) == -1){
				// or just delete it?
				error = true;
				break;
			}
		}
		// 遍历设置最大序号
		if(decode_qitem_key(it->key(), NULL, &seq_max) == -1){
			error = true;
			break;
		}
		// 增加计数
		count ++;
	}
	delete it;
	if(error){
		return -1;
	}
	
	if(count == 0){
	    // 队列为空，确保队列相关信息均删除
		this->binlogs->Delete(encode_qsize_key(name));
		qdel_one(this, name, QFRONT_SEQ);
		qdel_one(this, name, QBACK_SEQ);
	}else{
	    // 队列非空，更新队列尺寸和头部/尾部序号，确保队列信息正确
		this->binlogs->Put(encode_qsize_key(name), leveldb::Slice((char *)&count, sizeof(count)));
		qset_one(this, name, QFRONT_SEQ, Bytes(&seq_min, sizeof(seq_min)));
		qset_one(this, name, QBACK_SEQ, Bytes(&seq_max, sizeof(seq_max)));
	}

	// 提交事务
	leveldb::Status s = binlogs->commit();
	if(!s.ok()){
		log_error("Write error!");
		return -1;
	}
	return 0;
}

// 返回队列中指定索引区间的所有元素值
int SSDBImpl::qslice(const Bytes &name, int64_t begin, int64_t end,
		std::vector<std::string> *list)
{
	int ret;
	uint64_t seq_begin, seq_end;
	// 索引均是正序索引
	if(begin >= 0 && end >= 0){
		uint64_t tmp_seq;
		// 获取头部序号
		ret = qget_uint64(this->db, name, QFRONT_SEQ, &tmp_seq);
		if(ret != 1){
			return ret;
		}
		// 根据头部序号和索引计算指定区间的序号值
		seq_begin = tmp_seq + begin;
		seq_end = tmp_seq + end;
	// 索引均是倒序索引
	}else if(begin < 0 && end < 0){
		uint64_t tmp_seq;
		ret = qget_uint64(this->db, name, QBACK_SEQ, &tmp_seq);
		if(ret != 1){
			return ret;
		}
		seq_begin = tmp_seq + begin + 1;
		seq_end = tmp_seq + end + 1;
	// 索引一正一倒
	}else{
		uint64_t f_seq, b_seq;
		ret = qget_uint64(this->db, name, QFRONT_SEQ, &f_seq);
		if(ret != 1){
			return ret;
		}
		ret = qget_uint64(this->db, name, QBACK_SEQ, &b_seq);
		if(ret != 1){
			return ret;
		}
		if(begin >= 0){
			seq_begin = f_seq + begin;
		}else{
			seq_begin = b_seq + begin + 1;
		}
		if(end >= 0){
			seq_end = f_seq + end;
		}else{
			seq_end = b_seq + end + 1;
		}
	}
	
	// 遍历指定索引区间对应的序号区间，获取队列中的元素
	for(; seq_begin <= seq_end; seq_begin++){
		std::string item;
		ret = qget_by_seq(this->db, name, seq_begin, &item);
		if(ret == -1){
			return -1;
		}
		if(ret == 0){
			return 0;
		}
		list->push_back(item);
	}
	return 0;
}

// 根据索引从队列中获取元素
int SSDBImpl::qget(const Bytes &name, int64_t index, std::string *item){
	int ret;
	uint64_t seq;
	if(index >= 0){
	    // 从头部开始计算索引
		ret = qget_uint64(this->db, name, QFRONT_SEQ, &seq);
		seq += index;
	}else{
	    // 从尾部开始计算索引
		ret = qget_uint64(this->db, name, QBACK_SEQ, &seq);
		seq += index + 1;
	}
	if(ret == -1){
		return -1;
	}
	if(ret == 0){
		return 0;
	}
	
	// 根据序号获取元素
	ret = qget_by_seq(this->db, name, seq, item);
	return ret;
}
