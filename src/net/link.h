/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_LINK_H_
#define NET_LINK_H_

#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "../util/bytes.h"

#include "link_redis.h"

// 表示一个客户端的链接？看看再回来做更多解释
//
// 表示一个网络连接，可以从socket读取数据、向socket写入数据等操作，以及
// 解析请求数据等。应该在NetworkServer中用到的吧。
class Link{
	private:
	    // socket number？
		int sock;
		// 是否非阻塞
		bool noblock_;
		// 是否出现错误
		bool error_;
		// 接受到的数据
		std::vector<Bytes> recv_data;

        // 一个redis链接的指针，再RedisLink中处理从缓冲区读取请求/将响应写到缓冲区，以及
        // redis命令转换为ssdb命令
		RedisLink *redis;

        // 不清楚下面两个是干什么的
		static int min_recv_buf;
		static int min_send_buf;
	public:
	    // 客户端IP地址
		char remote_ip[INET_ADDRSTRLEN];
		// 客户端端口
		int remote_port;

        // 是否认证
		bool auth;
		// 忽略键的区间？不知道这是啥
		bool ignore_key_range;

        // 输入和输出缓冲区，其实应该先看看缓冲区的。。。
		Buffer *input;
		Buffer *output;
		
		// 创建时间，活跃时间
		double create_time;
		double active_time;

        // 构造和析构函数
		Link(bool is_server=false);
		~Link();
		// 关闭链接
		void close();
		// 设置nodelay
		void nodelay(bool enable=true);
		// noblock(true) is supposed to corperate with IO Multiplex,
		// otherwise, flush() may cause a lot unneccessary write calls.
		// 设置noblock
		void noblock(bool enable=true);
		// 设置keepalive
		void keepalive(bool enable=true);

        // 返回连接的文件描述符，再这里是socket的文件描述符
		int fd() const{
			return sock;
		}
		// 是否有错误
		bool error() const{
			return error_;
		}
		// 标记出现错误
		void mark_error(){
			error_ = true;
		}

        // 连接
		static Link* connect(const char *ip, int port);
		// 监听，这肯定是server模式才使用的
		static Link* listen(const char *ip, int port);
		// 开始接受请求
		Link* accept();

		// read network data info buffer
		// 将网络传输数据读取到输入缓冲区
		int read();
		// 将输出缓冲区的数据写到网络传输缓冲区
		int write();
		// flush buffered data to network
		// REQUIRES: nonblock
		// 将网络输出缓冲区的数据发送出去
		int flush();

		/**
		 * parse received data, and return -
		 * NULL: error
		 * empty vector: recv not ready
		 * vector<Bytes>: recv ready
		 */
		// 接收请求数据，每猜错的话这肯定是将输入缓冲区的数据进行解析，返回Bytes数组，应该会
		// 调用RedisLink的方法来接收数据
		const std::vector<Bytes>* recv();
		// wait until a response received.
		// 发送响应，应该是等待输出，并发送出去
		const std::vector<Bytes>* response();

        // 一组函数，用来发送不同格式的数据到网络
		// need to call flush to ensure all data has flush into network
		int send(const std::vector<std::string> &packet);
		int send(const std::vector<Bytes> &packet);
		int send(const Bytes &s1);
		int send(const Bytes &s1, const Bytes &s2);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5);

        // 上一次接收的数据
		const std::vector<Bytes>* last_recv(){
			return &recv_data;
		}
		
		/** these methods will send a request to the server, and wait until a response received.
		 * @return
		 * NULL: error
		 * vector<Bytes>: response ready
		 */
		// 下面这组方法将向服务器发送一个请求并等待响应结果
		const std::vector<Bytes>* request(const Bytes &s1);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5);
};

#endif
