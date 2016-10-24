/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "proc.h"
#include "server.h"
#include "../util/log.h"

ProcMap::ProcMap(){
}

ProcMap::~ProcMap(){
	proc_map_t::iterator it;
	for(it=proc_map.begin(); it!=proc_map.end(); it++){
	    // 把命令指针清理掉
		delete it->second;
	}
	// 清空map
	proc_map.clear();
}

// 为某个命令设置处理函数
void ProcMap::set_proc(const std::string &c, proc_t proc){
    // 默认使用线程处理请求
	this->set_proc(c, "t", proc);
}

void ProcMap::set_proc(const std::string &c, const char *sflags, proc_t proc){
	Command *cmd = this->get_proc(c);
	if(!cmd){
	    // 如果没有对应命令，新创建一个
		cmd = new Command();
		cmd->name = c;
		// 保存到命令map中
		proc_map[cmd->name] = cmd;
	}
	// 设置命令的处理函数
	cmd->proc = proc;
	// 设置命令的flag，表示命令是一个读命令，写命令，后台命令还是线程命令
	cmd->flags = 0;
	for(const char *p=sflags; *p!='\0'; p++){
		switch(*p){
			case 'r':
				cmd->flags |= Command::FLAG_READ;
				break;
			case 'w':
				cmd->flags |= Command::FLAG_WRITE;
				break;
			case 'b':
				cmd->flags |= Command::FLAG_BACKEND;
				break;
			case 't':
				cmd->flags |= Command::FLAG_THREAD;
				break;
		}
	}
}

// 根据命令获取处理命令
Command* ProcMap::get_proc(const Bytes &str){
	proc_map_t::iterator it = proc_map.find(str);
	if(it != proc_map.end()){
		return it->second;
	}
	return NULL;
}
