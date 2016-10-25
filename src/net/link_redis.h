/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_REDIS_LINK_H_
#define NET_REDIS_LINK_H_

#include <vector>
#include <string>
#include "../util/bytes.h"

// 请求描述？
struct RedisRequestDesc
{
    // 策略
	int strategy;
	// redis命令
	std::string redis_cmd;
	// ssdb命令
	std::string ssdb_cmd;
	// 返回类型
	int reply_type;
};

// 表示一个redis连接？
//
// 大致了解了这个类的功能，主要功能就是将redis协议的请求转换成ssdb的请求，转换过程中可能
// 需要针对命令的参数/返回结果等做一些处理。请求转换的过程是将输入缓冲区中的数据转换成Bytes
// 数组，以供后续处理。响应转换的过程是将string数组中表示的输出内容放到输出缓冲区中。在
// 这里没有处理任何网络相关的功能和流程，只是进行输入输出的转换以及格式解析和处理。
class RedisLink
{
private:
    // 命令
	std::string cmd;
	// 请求描述
	RedisRequestDesc *req_desc;

    // 这个需要看，是请求吗？
    //
    // 存储解析完毕的请求数据，最终请求解析完毕后会把recv_bytes返回
	std::vector<Bytes> recv_bytes;
	// 这个不知道是什么
	//
	// 在从redis命令切换为ssdb命令的过程中，使用recv_string作为中间存储，最终返回解析
	// 后的请求时会再次将recv_string中的内容放到recv_bytes中去返回
	std::vector<std::string> recv_string;
	// 解析请求
	int parse_req(Buffer *input);
	// 转换请求
	int convert_req();
	
public:
	RedisLink(){
	    // 请其描述对象的指针，会再convert_req函数中被赋值
		req_desc = NULL;
	}
	
	// 接受请求
	const std::vector<Bytes>* recv_req(Buffer *input);
	// 发送响应
	int send_resp(Buffer *output, const std::vector<std::string> &resp);
};

#endif
