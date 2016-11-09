/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "worker.h"
#include "link.h"
#include "proc.h"
#include "../util/log.h"
#include "../include.h"

ProcWorker::ProcWorker(const std::string &name){
	this->name = name;
}

void ProcWorker::init(){
	log_debug("%s %d init", this->name.c_str(), this->id);
}

// 处理任务
// 这个函数是在线程中在工作池中被调用，用于处理客户端请求，步骤如下：
// 1. 从客户端连接中获取请求数据；
// 2. 通过命令找到处理函数；
// 3. 传入请求数据，调用处理函数，获取返回结果；
// 4. 将返回结果发送到输出缓冲区，结束；
int ProcWorker::proc(ProcJob *job){
	const Request *req = job->link->last_recv();
	Response resp;
	
	proc_t p = job->cmd->proc;
	job->time_wait = 1000 * (millitime() - job->stime);
	job->result = (*p)(job->serv, job->link, *req, &resp);
	job->time_proc = 1000 * (millitime() - job->stime) - job->time_wait;

	if(job->link->send(resp.resp) == -1){
		job->result = PROC_ERROR;
	}else{
		log_debug("w:%.3f,p:%.3f, req: %s, resp: %s",
			job->time_wait, job->time_proc,
			serialize_req(*req).c_str(),
			serialize_req(resp.resp).c_str());
	}
	return 0;
}
