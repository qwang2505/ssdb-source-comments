/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <pthread.h>
#include "backend_dump.h"
#include "util/log.h"

BackendDump::BackendDump(SSDB *ssdb){
	this->ssdb = ssdb;
}

// 对象销毁的时候，说明后台dump完成了？
BackendDump::~BackendDump(){
	log_debug("BackendDump finalized");
}

// 处理后台dump命令，传入的是一个客户端连接指针。在处理中会启动
// 线程来进行真正的dump操作
void BackendDump::proc(const Link *link){
	log_info("accept dump client: %d", link->fd());
	// 创建新的运行参数
	struct run_arg *arg = new run_arg();
	// 设置客户端连接和用于dump的对象指针
	arg->link = link;
	arg->backend = this;

    // 启动新线程，在线程中进行真正的dump
	pthread_t tid;
	int err = pthread_create(&tid, NULL, &BackendDump::_run_thread, arg);
	if(err != 0){
		log_error("can't create thread: %s", strerror(err));
		delete link;
	}
}

// 在线程中运行，执行真正的dump操作
void* BackendDump::_run_thread(void *arg){
    // 拿到运行参数
	struct run_arg *p = (struct run_arg*)arg;
	// 拿到用于dump的对象的指针
	const BackendDump *backend = p->backend;
	// 拿到客户端连接指针
	Link *link = (Link *)p->link;
	delete p;

	// 设置客户端连接为阻塞
	link->noblock(false);

    // 获取客户端连接中的请求数据
	const std::vector<Bytes>* req = link->last_recv();

    // 从请求中获取命令参数，包括：start, end, limit
	std::string start = "";
	if(req->size() > 1){
		Bytes b = req->at(1);
		start.assign(b.data(), b.size());
	}
	if(start.empty()){
		start = "A";
	}
	std::string end = "";
	if(req->size() > 2){
		Bytes b = req->at(2);
		end.assign(b.data(), b.size());
	}
	uint64_t limit = 10;
	if(req->size() > 3){
		Bytes b = req->at(3);
		limit = b.Uint64();
	}

	log_info("fd: %d, begin to dump data: '%s', '%s', %" PRIu64 "",
		link->fd(), start.c_str(), end.c_str(), limit);

    // 获取到客户端连接的输出缓冲区
	Buffer *output = link->output;

	int count = 0;
	bool quit = false;
	// 获取迭代器来遍历导出数据
	Iterator *it = backend->ssdb->iterator(start, end, limit);
	
	// 发送dump开始的信息，send会把内容放到输出缓冲区
	link->send("begin");
	// 不推出的情况下，遍历数据并导出
	while(!quit){
	    // 没有下一个了，输出结果的数量，输出end表示数据结束，推出循环
		if(!it->next()){
			quit = true;
			char buf[20];
			snprintf(buf, sizeof(buf), "%d", count);
			link->send("end", buf);
		}else{
		    // 增加结果数量的计数器
			count ++;
			// 获取到key和value
			Bytes key = it->key();
			Bytes val = it->val();

            // 将key/value放到输出缓冲区
			output->append_record("set");
			output->append_record(key);
			output->append_record(val);
			// 加换行符表示一条数据结束
			output->append('\n');

            // 输出缓冲区没有超过32KB就继续，如果超过就县flush再继续
			if(output->size() < 32 * 1024){
				continue;
			}
		}

        // 将输出缓冲区的数据发送到网络
		if(link->flush() == -1){
			log_error("fd: %d, send error: %s", link->fd(), strerror(errno));
			break;
		}
	}
	// wait for client to close connection,
	// or client may get a "Connection reset by peer" error.
	// 这是什么意思呢？客户端在dump之后会发送一个空消息来断开连接？
	link->read();

	log_info("fd: %d, delete link", link->fd());
	delete link;
	delete it;
	return (void *)NULL;
}
