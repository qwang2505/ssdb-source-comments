/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_PROC_H_
#define NET_PROC_H_

#include <vector>
#include "resp.h"
#include "../util/bytes.h"

class Link;
class NetworkServer;

#define PROC_OK			0
#define PROC_ERROR		-1
#define PROC_THREAD     1
#define PROC_BACKEND	100

// 定义命令处理函数的宏
#define DEF_PROC(f) int proc_##f(NetworkServer *net, Link *link, const Request &req, Response *resp)

// Request就是个Bytes数组
typedef std::vector<Bytes> Request;
// 请求处理函数
typedef int (*proc_t)(NetworkServer *net, Link *link, const Request &req, Response *resp);

// 定义一个命令
struct Command{
    // flag表示这个命令是个什么类型的命令
	static const int FLAG_READ		= (1 << 0);
	static const int FLAG_WRITE		= (1 << 1);
	static const int FLAG_BACKEND	= (1 << 2);
	static const int FLAG_THREAD	= (1 << 3);

	std::string name;
	int flags;
	// 处理此命令的函数
	proc_t proc;
	uint64_t calls;
	double time_wait;
	double time_proc;
	
	Command(){
		flags = 0;
		proc = NULL;
		calls = 0;
		time_wait = 0;
		time_proc = 0;
	}
};

// 一个处理请求的job
struct ProcJob{
	int result;
	// 网络服务器
	NetworkServer *serv;
	// 网络链接？
	Link *link;
	// 处理请求的命令
	Command *cmd;
	double stime;
	double time_wait;
	double time_proc;
	
	ProcJob(){
		result = 0;
		serv = NULL;
		link = NULL;
		cmd = NULL;
		stime = 0;
		time_wait = 0;
		time_proc = 0;
	}
};


// 判定两个byte是否相等，为啥要定义成结构体呢？
struct BytesEqual{
	bool operator()(const Bytes &s1, const Bytes &s2) const {
		return (bool)(s1.compare(s2) == 0);
	}
};
// 求字符串的哈希值
struct BytesHash{
	size_t operator()(const Bytes &s1) const {
		unsigned long __h = 0;
		const char *p = s1.data();
		for (int i=0 ; i<s1.size(); i++)
			__h = 5*__h + p[i];
		return size_t(__h);
	}
};


// proc_map_t貌似是个字符串到command指针的影射，看用法在具体了解
#define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)
#if GCC_VERSION >= 403
	#include <tr1/unordered_map>
	typedef std::tr1::unordered_map<Bytes, Command *, BytesHash, BytesEqual> proc_map_t;
#else
	#ifdef NEW_MAC
		#include <unordered_map>
		typedef std::unordered_map<Bytes, Command *, BytesHash, BytesEqual> proc_map_t;
	#else
		#include <ext/hash_map>
		typedef __gnu_cxx::hash_map<Bytes, Command *, BytesHash, BytesEqual> proc_map_t;
	#endif
#endif


// 定义请求处理的映射关系，管理命令的处理函数
class ProcMap
{
private:
    // 具体的命令映射
	proc_map_t proc_map;

public:
	ProcMap();
	~ProcMap();
	// 设置某个命令的处理函数，这里应该会根据flag来设置
	void set_proc(const std::string &cmd, const char *sflags, proc_t proc);
	// 设置命令的处理函数
	void set_proc(const std::string &cmd, proc_t proc);
	Command* get_proc(const Bytes &str);
	
	// 迭代器来获取所有命令
	proc_map_t::iterator begin(){
		return proc_map.begin();
	}
	proc_map_t::iterator end(){
		return proc_map.end();
	}
};



#include "../util/strings.h"

// 持久化请求，需要看在哪里用到
template<class T>
static std::string serialize_req(T &req){
	std::string ret;
	char buf[50];
	for(int i=0; i<req.size(); i++){
		if(i >= 5 && i < req.size() - 1){
			sprintf(buf, "[%d more...]", (int)req.size() - i);
			ret.append(buf);
			break;
		}
		if(((req[0] == "get" || req[0] == "set") && i == 1) || req[i].size() < 50){
			if(req[i].size() == 0){
				ret.append("\"\"");
			}else{
				std::string h = hexmem(req[i].data(), req[i].size());
				ret.append(h);
			}
		}else{
			sprintf(buf, "[%d]", (int)req[i].size());
			ret.append(buf);
		}
		if(i < req.size() - 1){
			ret.append(" ");
		}
	}
	return ret;
}

#endif
