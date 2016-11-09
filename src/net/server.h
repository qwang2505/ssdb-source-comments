/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_SERVER_H_
#define NET_SERVER_H_

#include "../include.h"
#include <string>
#include <vector>

#include "fde.h"
#include "proc.h"
#include "worker.h"

class Link;
class Config;
class IpFilter;
class Fdevents;

// 网络连接列表
typedef std::vector<Link *> ready_list_t;

// 网络服务器，在这里进行连接/请求处理等操作
class NetworkServer
{
private:
    // 时钟周期
	int tick_interval;
	// 这个不知道是什么
	int status_report_ticks;

	//Config *conf;
	// 服务端的连接对象指针
	Link *serv_link;
	// IP过滤器，应该用于访问限制
	IpFilter *ip_filter;
	// 事件对象指针
	Fdevents *fdes;

    // 接收客户端请求？
	Link* accept_link();
	// 处理请求？
	int proc_result(ProcJob *job, ready_list_t *ready_list);
	// 处理客户端事件
	int proc_client_event(const Fdevent *fde, ready_list_t *ready_list);

    // 处理请求
	void proc(ProcJob *job);

    // 读线程数量
	int num_readers;
	// 写线程数量
	int num_writers;
	// 用于读操作的工作池
	ProcWorkerPool *writer;
	// 用于写操作的工作池
	ProcWorkerPool *reader;

    // 私有构造函数，初始化全部从init函数来初始化
	NetworkServer();

protected:
    // 打印使用帮助信息
	void usage(int argc, char **argv);

public:
    // 这是什么数据？
	void *data;
	// 处理命令的处理函数map
	ProcMap proc_map;
	// 网络请求链接数量
	int link_count;
	// 是否需要认证
	bool need_auth;
	// 密码
	std::string password;

	~NetworkServer();

	// could be called only once
	static NetworkServer* init(const char *conf_file, int num_readers=-1, int num_writers=-1);
	static NetworkServer* init(const Config &conf, int num_readers=-1, int num_writers=-1);
	// 开始工作，开始接收请求
	void serve();
};


#endif
