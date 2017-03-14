/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SERVER_H_
#define SSDB_SERVER_H_

#include "include.h"
#include <map>
#include <vector>
#include <string>
#include "ssdb/ssdb_impl.h"
#include "ssdb/ttl.h"
#include "backend_dump.h"
#include "backend_sync.h"
#include "slave.h"
#include "net/server.h"
#include "cluster.h"

// SSDB服务对象
class SSDBServer
{
private:
    // 用于注册命令处理函数
	void reg_procs(NetworkServer *net);
	
	// 记录KV类型的数据的开始字符串和结束字符串
	// 集群中各服务器存储一个区间的key value，并且可能在不同的服务器之间进行迁移。
	std::string kv_range_s;
	std::string kv_range_e;
	
	SSDB *meta;

public:
	SSDBImpl *ssdb;
	BackendDump *backend_dump;
	BackendSync *backend_sync;
	ExpirationHandler *expiration;
	std::vector<Slave *> slaves;
	Cluster *cluster;

	SSDBServer(SSDB *ssdb, SSDB *meta, const Config &conf, NetworkServer *net);
	~SSDBServer();

	int set_kv_range(const std::string &s, const std::string &e);
	int get_kv_range(std::string *s, std::string *e);
	bool in_kv_range(const std::string &key);
	bool in_kv_range(const Bytes &key);
};


#define CHECK_KV_KEY_RANGE(n) do{ \
		if(!link->ignore_key_range && req.size() > n){ \
			if(!serv->in_kv_range(req[n])){ \
				resp->push_back("out_of_range"); \
				return 0; \
			} \
		} \
	}while(0)

#define CHECK_NUM_PARAMS(n) do{ \
		if(req.size() < n){ \
			resp->push_back("client_error"); \
			resp->push_back("wrong number of arguments"); \
			return 0; \
		} \
	}while(0)

#endif
