/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "resp.h"
#include <stdio.h>

int Response::size() const{
	return (int)resp.size();
}

void Response::push_back(const std::string &s){
	resp.push_back(s);
}

void Response::add(int s){
	add((int64_t)s);
}

// 在返回结果中加一个数字。PRId64用于跨平台格式化数据
void Response::add(int64_t s){
	char buf[20];
	sprintf(buf, "%" PRId64 "", s);
	resp.push_back(buf);
}

// 添加一个unsigned 64 int
void Response::add(uint64_t s){
	char buf[20];
	sprintf(buf, "%" PRIu64 "", s);
	resp.push_back(buf);
}

// 添加一个double
void Response::add(double s){
	char buf[30];
	snprintf(buf, sizeof(buf), "%f", s);
	resp.push_back(buf);
}

// 添加一个string到返回结果
void Response::add(const std::string &s){
	resp.push_back(s);
}

// 添加返回结果的状态
void Response::reply_status(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else{
		resp.push_back("ok");
	}
}

// 返回bool结果
void Response::reply_bool(int status, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
		if(errmsg){
			resp.push_back(errmsg);
		}
	}else if(status == 0){
		resp.push_back("ok");
		resp.push_back("0");
	}else{
		resp.push_back("ok");
		resp.push_back("1");
	}
}

// 返回int结果
void Response::reply_int(int status, int64_t val){
	if(status == -1){
		resp.push_back("error");
	}else{
		resp.push_back("ok");
		this->add(val);
	}
}

// 返回get的结果，结果中是字符串
void Response::reply_get(int status, const std::string *val, const char *errmsg){
	if(status == -1){
		resp.push_back("error");
	}else if(status == 0){
		resp.push_back("not_found");
	}else{
		resp.push_back("ok");
		if(val){
			resp.push_back(*val);
		}
		return;
	}
	if(errmsg){
		resp.push_back(errmsg);
	}
} 

// 返回list内容，list的每个内容是string
void Response::reply_list(int status, const std::vector<std::string> &list){
	if(status == -1){
		resp.push_back("error");
	}else{
		resp.push_back("ok");
		for(int i=0; i<list.size(); i++){
			resp.push_back(list[i]);
		}
	}
}

