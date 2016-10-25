/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdarg.h>
#include <sys/socket.h>

#include "link.h"

#include "link_redis.cpp"

// 最大数据包的大小，32M
#define MAX_PACKET_SIZE		32 * 1024 * 1024
// 空缓冲区的大小，为啥不是0呢？这是个最初的缓冲区大小。buffer可以自己增加和缩小的，
// 初始先初始化的小一点，避免浪费
#define ZERO_BUFFER_SIZE	8

// 这是什么？用到再看
int Link::min_recv_buf = 8 * 1024;
int Link::min_send_buf = 8 * 1024;


// 初始化一个连接
Link::Link(bool is_server){
	redis = NULL;

	sock = -1;
	noblock_ = false;
	error_ = false;
	remote_ip[0] = '\0';
	remote_port = -1;
	auth = false;
	ignore_key_range = false;
	
	if(is_server){
	    // 为啥server模式要将缓冲区初始化成空指针？
		input = output = NULL;
	}else{
		// alloc memory lazily
		//input = new Buffer(Link::min_recv_buf);
		//output = new Buffer(Link::min_send_buf);
		// 客户端模式，初始化用到的缓冲区
		input = new Buffer(ZERO_BUFFER_SIZE);
		output = new Buffer(ZERO_BUFFER_SIZE);
	}
}

// 析构函数，释放必须的内存
Link::~Link(){
	if(redis){
		delete redis;
	}
	if(input){
		delete input;
	}
	if(output){
		delete output;
	}
	this->close();
}

// 关闭socket，直接调用接口函数关闭就可以了
void Link::close(){
	if(sock >= 0){
		::close(sock);
	}
}

// 设置socket属性
void Link::nodelay(bool enable){
	int opt = enable? 1 : 0;
	::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (void *)&opt, sizeof(opt));
}

// 设置socket属性
void Link::keepalive(bool enable){
	int opt = enable? 1 : 0;
	::setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, (void *)&opt, sizeof(opt));
}

// 设置socket属性
void Link::noblock(bool enable){
	noblock_ = enable;
	if(enable){
		::fcntl(sock, F_SETFL, O_NONBLOCK | O_RDWR);
	}else{
		::fcntl(sock, F_SETFL, O_RDWR);
	}
}


// 连接，这应该是在client中用到的吧，用于根据ip和端口连接server
// 这是个static函数
// 如果连接服务器成功，返回一个Link对象指针
Link* Link::connect(const char *ip, int port){
	Link *link;
	int sock = -1;

    // 初始化地址结构
	struct sockaddr_in addr;
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons((short)port);
	inet_pton(AF_INET, ip, &addr.sin_addr);

    // 创建socket连接
	if((sock = ::socket(AF_INET, SOCK_STREAM, 0)) == -1){
		goto sock_err;
	}
	// 使用之前创建的socket去连接服务器
	if(::connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1){
		goto sock_err;
	}

	//log_debug("fd: %d, connect to %s:%d", sock, ip, port);
	// 创建新的link对象，表示一个网络连接
	link = new Link();
	// 设置socket
	link->sock = sock;
	link->keepalive(true);
	return link;
sock_err:
	//log_debug("connect to %s:%d failed: %s", ip, port, strerror(errno));
	if(sock >= 0){
	    // 出现错误，关闭socket，返回空指针
		::close(sock);
	}
	return NULL;
}

// 监听某个ip及端口，server模式用到
Link* Link::listen(const char *ip, int port){
	Link *link;
	int sock = -1;

	int opt = 1;
	struct sockaddr_in addr;
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons((short)port);
	inet_pton(AF_INET, ip, &addr.sin_addr);

	if((sock = ::socket(AF_INET, SOCK_STREAM, 0)) == -1){
		goto sock_err;
	}
	if(::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1){
		goto sock_err;
	}
	if(::bind(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1){
		goto sock_err;
	}
	if(::listen(sock, 1024) == -1){
		goto sock_err;
	}
	//log_debug("server socket fd: %d, listen on: %s:%d", sock, ip, port);

	link = new Link(true);
	link->sock = sock;
	snprintf(link->remote_ip, sizeof(link->remote_ip), "%s", ip);
	link->remote_port = port;
	return link;
sock_err:
	//log_debug("listen %s:%d failed: %s", ip, port, strerror(errno));
	if(sock >= 0){
		::close(sock);
	}
	return NULL;
}

// 开始接收客户端连接
Link* Link::accept(){
	Link *link;
	int client_sock;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);

    // 开始接收客户端连接。如果操作失败且错误不是特定的错误，则一直循环来accept
	while((client_sock = ::accept(sock, (struct sockaddr *)&addr, &addrlen)) == -1){
		if(errno != EINTR){
			//log_error("socket %d accept failed: %s", sock, strerror(errno));
			return NULL;
		}
	}
	// 到这里已经开始接收请求了

	// 设置网络连接的配置，不太清除这里设置了什么配置，回头再看
	struct linger opt = {1, 0};
	int ret = ::setsockopt(client_sock, SOL_SOCKET, SO_LINGER, (void *)&opt, sizeof(opt));
	if (ret != 0) {
		//log_error("socket %d set linger failed: %s", client_sock, strerror(errno));
	}

    // 创建新连接
	link = new Link();
	link->sock = client_sock;
	link->keepalive(true);
	inet_ntop(AF_INET, &addr.sin_addr, link->remote_ip, sizeof(link->remote_ip));
	// 保存客户端地址
	link->remote_port = ntohs(addr.sin_port);
	return link;
}

// 从网络socket读取数据，放到输入缓冲区中
// 返回读取了多少数据
int Link::read(){
    // 输入缓冲区太小了，增大点
	if(input->total() == ZERO_BUFFER_SIZE){
		input->grow();
	}
	int ret = 0;
	int want;
	// 在nice中会根据情况移动数据，以把空闲数据区域空出来
	input->nice();

    // 循环从socket中读取数据到输入缓冲区
	while((want = input->space()) > 0){
		// test
		//want = 1;
		// 从socket读取数据到输入缓冲区
		int len = ::read(sock, input->slot(), want);
		if(len == -1){
		    // 关于下面几个信号相关的，以后再详细了解
			if(errno == EINTR){
				continue;
			}else if(errno == EWOULDBLOCK){
				break;
			}else{
				//log_debug("fd: %d, read: -1, want: %d, error: %s", sock, want, strerror(errno));
				return -1;
			}
		}else{
			//log_debug("fd: %d, want=%d, read: %d", sock, want, len);
			// 读取完毕了，剩余数据长度为0
			if(len == 0){
				return 0;
			}
			// 读取了len长度的数据，更新输入缓冲区指针
			ret += len;
			input->incr(len);
		}
		// TODO 这个设置真的很费解
		if(!noblock_){
			break;
		}
	}
	//log_debug("read %d", ret);
	return ret;
}

// 将输出缓冲区的数据写到socket
int Link::write(){
    // 这里为啥也要grow？这里不会操作output
	if(output->total() == ZERO_BUFFER_SIZE){
		output->grow();
	}
	int ret = 0;
	int want;
	while((want = output->size()) > 0){
		// test
		//want = 1;
		int len = ::write(sock, output->data(), want);
		if(len == -1){
			if(errno == EINTR){
				continue;
			}else if(errno == EWOULDBLOCK){
				break;
			}else{
				//log_debug("fd: %d, write: -1, error: %s", sock, strerror(errno));
				return -1;
			}
		}else{
			//log_debug("fd: %d, want: %d, write: %d", sock, want, len);
			if(len == 0){
				// ?
				break;
			}
			ret += len;
			output->decr(len);
		}
		if(!noblock_){
			break;
		}
	}
	output->nice();
	return ret;
}

// 将输出缓冲区的数据写到socket，写了之后就相当于发送出去了
int Link::flush(){
	int len = 0;
	while(!output->empty()){
		int ret = this->write();
		if(ret == -1){
			return -1;
		}
		len += ret;
	}
	return len;
}

// 接收数据，从输入缓冲区中读取数据，并写入到Bytes数组返回
const std::vector<Bytes>* Link::recv(){
	this->recv_data.clear();

    // 输入缓冲区的数据是空的，没啥需要接收的
	if(input->empty()){
		return &this->recv_data;
	}

    // 输入缓冲区有数据，读出来
	// TODO: 记住上回的解析状态
	int parsed = 0;
	int size = input->size();
	char *head = input->data();
	
	// Redis protocol supports
	// 支持redis协议
	if(head[0] == '*'){
		if(redis == NULL){
			redis = new RedisLink();
		}
		// 调用RedisLink方法来解析数据，解析到Bytes数组
		// 解析成功则返回
		const std::vector<Bytes> *ret = redis->recv_req(input);
		if(ret){
			this->recv_data = *ret;
			return &this->recv_data;
		}else{
			return NULL;
		}
	}

	// ignore leading empty lines
	// 忽略空行
	while(size > 0 && (head[0] == '\n' || head[0] == '\r')){
		head ++;
		size --;
		parsed ++;
	}

    // 下面的解析过程和RedisLink基本是一样的
	while(size > 0){
	    // 找到body的开始位置，\n之后
		char *body = (char *)memchr(head, '\n', size);
		if(body == NULL){
			break;
		}
		body ++;

		int head_len = body - head;
		// 没有更多数据，直接返回，不过还是要把已经解析的记录下来
		// 数据后面的head区域为\n或者\r\n表示一个数据包结束了，此时
		// 可以返回了
		if(head_len == 1 || (head_len == 2 && head[0] == '\r')){
			// packet end
			parsed += head_len;
			input->decr(parsed);
			// 正确读取到数据的返回在这里
			return &this->recv_data;;
		}
		if(head[0] < '0' || head[0] > '9'){
			//log_warn("bad format");
			return NULL;
		}

        // 读HEAD部分
		char head_str[20];
		if(head_len > (int)sizeof(head_str) - 1){
			return NULL;
		}
		memcpy(head_str, head, head_len - 1); // no '\n'
		head_str[head_len - 1] = '\0';

        // 获取数据内容的长度
		int body_len = atoi(head_str);
		if(body_len < 0){
			//log_warn("bad format");
			return NULL;
		}
		//log_debug("size: %d, head_len: %d, body_len: %d", size, head_len, body_len);
		size -= head_len + body_len;
		if(size < 0){
			break;
		}

        // 读取body
		this->recv_data.push_back(Bytes(body, body_len));

        // 更新head指针和parsed记录
		head += head_len + body_len;
		parsed += head_len + body_len;
		// 跳过body后面的\n或者\r\n
		if(size > 0 && head[0] == '\n'){
			head += 1;
			size -= 1;
			parsed += 1;
		}else if(size > 1 && head[0] == '\r' && head[1] == '\n'){
			head += 2;
			size -= 2;
			parsed += 2;
		}else{
			break;
		}
		// 达到了数据包的最大范围，不读取
		if(parsed > MAX_PACKET_SIZE){
			 //log_warn("fd: %d, exceed max packet size, parsed: %d", this->sock, parsed);
			 return NULL;
		}
	}

    // 更新输入缓冲区大小
	if(input->space() == 0){
		input->nice();
		if(input->space() == 0){
			if(input->grow() == -1){
				//log_error("fd: %d, unable to resize input buffer!", this->sock);
				return NULL;
			}
			//log_debug("fd: %d, resize input buffer, %s", this->sock, input->stats().c_str());
		}
	}

	// not ready
	// 到这里应该是没有数据需要读取的情况
	this->recv_data.clear();
	return &this->recv_data;
}

// 发送响应，将字符串数组中的数据发送到输出缓冲区 
int Link::send(const std::vector<std::string> &resp){
	if(resp.empty()){
		return 0;
	}
	// Redis protocol supports
	if(this->redis){
		return this->redis->send_resp(this->output, resp);
	}
	
	for(int i=0; i<resp.size(); i++){
		output->append_record(resp[i]);
	}
	// 输出数据的最后还要加一个换行，表示一个数据包结束了
	output->append('\n');
	return 0;
}

// 发送数据到输出缓冲区
int Link::send(const std::vector<Bytes> &resp){
	for(int i=0; i<resp.size(); i++){
		output->append_record(resp[i]);
	}
	output->append('\n');
	return 0;
}

// 发送数据到输出缓冲区
int Link::send(const Bytes &s1){
	output->append_record(s1);
	output->append('\n');
	return 0;
}

// 发送数据到输出缓冲区
int Link::send(const Bytes &s1, const Bytes &s2){
	output->append_record(s1);
	output->append_record(s2);
	output->append('\n');
	return 0;
}

// 发送数据到输出缓冲区
int Link::send(const Bytes &s1, const Bytes &s2, const Bytes &s3){
	output->append_record(s1);
	output->append_record(s2);
	output->append_record(s3);
	output->append('\n');
	return 0;
}

// 发送数据到输出缓冲区
int Link::send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4){
	output->append_record(s1);
	output->append_record(s2);
	output->append_record(s3);
	output->append_record(s4);
	output->append('\n');
	return 0;
}

// 发送数据到输出缓冲区
int Link::send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5){
	output->append_record(s1);
	output->append_record(s2);
	output->append_record(s3);
	output->append_record(s4);
	output->append_record(s5);
	output->append('\n');
	return 0;
}

// 等待响应并接收后读取到Bytes数组中
const std::vector<Bytes>* Link::response(){
    // 等待直到从socket中读取数据
	while(1){
		const std::vector<Bytes> *resp = this->recv();
		if(resp == NULL){
			return NULL;
		}else if(resp->empty()){
		    // 没读取到数据，从socket中读取数据到输入缓冲区
			if(this->read() <= 0){
				return NULL;
			}
		}else{
			return resp;
		}
	}
	return NULL;
}

// 发送一个Bytes的数据，并等待响应
const std::vector<Bytes>* Link::request(const Bytes &s1){
    // 将要发送的数据写到输出缓冲区
	if(this->send(s1) == -1){
		return NULL;
	}
	// 将输出缓冲区的数据写到socket
	if(this->flush() == -1){
		return NULL;
	}
	// 等待响应结果
	return this->response();
}

// 发送两个Bytes的数据，并等待响应
const std::vector<Bytes>* Link::request(const Bytes &s1, const Bytes &s2){
	if(this->send(s1, s2) == -1){
		return NULL;
	}
	if(this->flush() == -1){
		return NULL;
	}
	return this->response();
}

// 发送三个Bytes的数据，并等待响应
const std::vector<Bytes>* Link::request(const Bytes &s1, const Bytes &s2, const Bytes &s3){
	if(this->send(s1, s2, s3) == -1){
		return NULL;
	}
	if(this->flush() == -1){
		return NULL;
	}
	return this->response();
}

// 发送四个Bytes的数据，并等待响应
const std::vector<Bytes>* Link::request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4){
	if(this->send(s1, s2, s3, s4) == -1){
		return NULL;
	}
	if(this->flush() == -1){
		return NULL;
	}
	return this->response();
}

// 发送五个Bytes的数据，并等待响应
const std::vector<Bytes>* Link::request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5){
	if(this->send(s1, s2, s3, s4, s5) == -1){
		return NULL;
	}
	if(this->flush() == -1){
		return NULL;
	}
	return this->response();
}

#if 0
int main(){
	//Link link;
	//link.listen("127.0.0.1", 8888);
	Link *link = Link::connect("127.0.0.1", 8080);
	printf("%d\n", link);
	getchar();
	return 0;
}
#endif
