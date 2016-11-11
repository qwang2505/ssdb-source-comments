/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BINLOG_H_
#define SSDB_BINLOG_H_

#include <string>
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "../util/thread.h"
#include "../util/bytes.h"


// 标示一条操作日志，其中包括操作序列号、操作类型、操作命令、操作的数据的key
class Binlog{
private:
	std::string buf;
	// 头部的长度，包括一个uint64_t的序列号，一个字节标示类型，一个字节标示命令
	static const unsigned int HEADER_LEN = sizeof(uint64_t) + 2;
public:
	Binlog(){}
	Binlog(uint64_t seq, char type, char cmd, const leveldb::Slice &key);
		
	int load(const Bytes &s);
	int load(const leveldb::Slice &s);
	int load(const std::string &s);

	uint64_t seq() const;
	char type() const;
	char cmd() const;
	const Bytes key() const;

	const char* data() const{
		return buf.data();
	}
	int size() const{
		return (int)buf.size();
	}
	const std::string repr() const{
		return this->buf;
	}
	std::string dumps() const;
};

// circular queue
// 操作日志队列，是个环形队列
// 主要用于进行事务的控制，在事务开始后会加锁，将操作缓存起来，然后
// 提交的时候再统一进行写入数据库的操作。每条操作会产生两个写入：一个
// 是真正的数据，一个是操作日志
class BinlogQueue{
private:
    // NDEBUG宏是用于控制assert的行为，如果定义，则assert不会起作用
    // 编译的时候会定义
#ifdef NDEBUG
    // 如果是debug，多一点
	static const int LOG_QUEUE_SIZE  = 10 * 1000 * 1000;
#else
    // 队列长度是10000条
	static const int LOG_QUEUE_SIZE  = 10000;
#endif
	leveldb::DB *db;
	uint64_t min_seq;
	uint64_t last_seq;
	uint64_t tran_seq;
	int capacity;
	leveldb::WriteBatch batch;

	volatile bool thread_quit;
	static void* log_clean_thread_func(void *arg);
	int del(uint64_t seq);
	// [start, end] includesive
	int del_range(uint64_t start, uint64_t end);
		
	void merge();
	bool enabled;
public:
    // 线程锁
	Mutex mutex;

	BinlogQueue(leveldb::DB *db, bool enabled=true);
	~BinlogQueue();
	void begin();
	void rollback();
	leveldb::Status commit();
	// leveldb put
	void Put(const leveldb::Slice& key, const leveldb::Slice& value);
	// leveldb delete
	void Delete(const leveldb::Slice& key);
	void add_log(char type, char cmd, const leveldb::Slice &key);
	void add_log(char type, char cmd, const std::string &key);
		
	int get(uint64_t seq, Binlog *log) const;
	int update(uint64_t seq, char type, char cmd, const std::string &key);
		
	void flush();
		
	/** @returns
	 1 : log.seq greater than or equal to seq
	 0 : not found
	 -1: error
	 */
	int find_next(uint64_t seq, Binlog *log) const;
	int find_last(Binlog *log) const;
		
	std::string stats() const;
};

// 事务支持。使用操作日志队列来存储事务中的操作
class Transaction{
private:
	BinlogQueue *logs;
public:
	Transaction(BinlogQueue *logs){
		this->logs = logs;
		// 加锁
		logs->mutex.lock();
		// 开始事务
		logs->begin();
	}
	
	~Transaction(){
		// it is safe to call rollback after commit
		logs->rollback();
		logs->mutex.unlock();
	}
};


#endif
