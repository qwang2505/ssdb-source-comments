/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BACKEND_SYNC_H_
#define SSDB_BACKEND_SYNC_H_

#include "include.h"
#include <vector>
#include <string>
#include <map>

#include "ssdb/ssdb_impl.h"
#include "ssdb/binlog.h"
#include "net/link.h"
#include "util/thread.h"

// 管理主从同步
class BackendSync{
private:
	struct Client;
private:
	std::vector<Client *> clients;
	std::vector<Client *> clients_tmp;

    // 线程运行参数
	struct run_arg{
		const Link *link;
		const BackendSync *backend;
	};
	// 是否退出线程运行
	volatile bool thread_quit;
	// 线程函数
	static void* _run_thread(void *arg);
	Mutex mutex;
	// 线程和client的map
	std::map<pthread_t, Client *> workers;
	SSDBImpl *ssdb;
	int sync_speed;
public:
	BackendSync(SSDBImpl *ssdb, int sync_speed);
	~BackendSync();
	void proc(const Link *link);
	
	std::vector<std::string> stats();
};

// 定义主从同步的客户端
struct BackendSync::Client{
	static const int INIT = 0;
	static const int OUT_OF_SYNC = 1;
	static const int COPY = 2;
	static const int SYNC = 4;

	int status;
	Link *link;
	uint64_t last_seq;
	uint64_t last_noop_seq;
	std::string last_key;
	const BackendSync *backend;
	bool is_mirror;
	
	Iterator *iter;

	Client(const BackendSync *backend);
	~Client();
	void init();
	void reset();
	void noop();
	int copy();
	int sync(BinlogQueue *logs);

	std::string stats();
};

#endif
