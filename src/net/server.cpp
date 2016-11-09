/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "server.h"
#include "../util/strings.h"
#include "../util/file.h"
#include "../util/config.h"
#include "../util/log.h"
#include "../util/ip_filter.h"
#include "link.h"
#include <vector>

static DEF_PROC(ping);
static DEF_PROC(info);
static DEF_PROC(auth);

// 时钟周期
#define TICK_INTERVAL          100 // ms
// 隔多久汇报一次状态，这里定义的是每隔5分钟汇报一次状态
#define STATUS_REPORT_TICKS    (300 * 1000/TICK_INTERVAL) // second
static const int READER_THREADS = 10;
static const int WRITER_THREADS = 1;

// 用全局静态变量来处理退出信号
volatile bool quit = false;
// 这个是用来干什么的？
volatile uint32_t g_ticks = 0;

// 注册对信号的处理，这里只要设置全局标志就可以了
void signal_handler(int sig){
	switch(sig){
		case SIGTERM:
		case SIGINT:{
			quit = true;
			break;
		}
		// 每次接收到时钟信号均将计数器增加，用于判断命令执行是否超时？
		case SIGALRM:{
			g_ticks ++;
			break;
		}
	}
}

// 初始化网络服务器，应该是私有的初始化函数
NetworkServer::NetworkServer(){
	num_readers = READER_THREADS;
	num_writers = WRITER_THREADS;
	
	tick_interval = TICK_INTERVAL;
	status_report_ticks = STATUS_REPORT_TICKS;

	//conf = NULL;
	serv_link = NULL;
	link_count = 0;

    // 初始化事件相关
	fdes = new Fdevents();
	ip_filter = new IpFilter();

	// add built-in procs, can be overridden
	proc_map.set_proc("ping", "r", proc_ping);
	proc_map.set_proc("info", "r", proc_info);
	proc_map.set_proc("auth", "r", proc_auth);

    // 设置信号处理
	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);
#ifndef __CYGWIN__
    // 设置定时期和时钟信号的处理函数
	signal(SIGALRM, signal_handler);
	{
		struct itimerval tv;
		// it_interval用于指定每隔多久发送一次信号
		// 这里指定的是每隔100毫秒发送一次时钟信号，可以理解成对于程序来说，时钟时间
		// 是100ms一次
		tv.it_interval.tv_sec = (TICK_INTERVAL / 1000);
		tv.it_interval.tv_usec = (TICK_INTERVAL % 1000) * 1000;
		// it_value指定第一次发送信号距离现在的时间
		tv.it_value.tv_sec = 1;
		tv.it_value.tv_usec = 0;
		// 设置定时信号，这里使用真实时钟
		setitimer(ITIMER_REAL, &tv, NULL);
	}
#endif
}
	
NetworkServer::~NetworkServer(){
	//delete conf;
	delete serv_link;
	delete fdes;
	delete ip_filter;

	writer->stop();
	delete writer;
	reader->stop();
	delete reader;
}

NetworkServer* NetworkServer::init(const char *conf_file, int num_readers, int num_writers){
	if(!is_file(conf_file)){
		fprintf(stderr, "'%s' is not a file or not exists!\n", conf_file);
		exit(1);
	}

	Config *conf = Config::load(conf_file);
	if(!conf){
		fprintf(stderr, "error loading conf file: '%s'\n", conf_file);
		exit(1);
	}
	{
	    // 切换到工作目录，默认为配置文件所在目录
		std::string conf_dir = real_dirname(conf_file);
		if(chdir(conf_dir.c_str()) == -1){
			fprintf(stderr, "error chdir: %s\n", conf_dir.c_str());
			exit(1);
		}
	}
	NetworkServer* serv = init(*conf, num_readers, num_writers);
	delete conf;
	return serv;
}

// 初始化网络服务器：使用配置进行初始化，开始监听指定ip和端口
NetworkServer* NetworkServer::init(const Config &conf, int num_readers, int num_writers){
	static bool inited = false;
	if(inited){
		return NULL;
	}
	inited = true;
	
	NetworkServer *serv = new NetworkServer();
	if(num_readers >= 0){
		serv->num_readers = num_readers;
	}
	if(num_writers >= 0){
		serv->num_writers = num_writers;
	}
	// init ip_filter
	{
		Config *cc = (Config *)conf.get("server");
		if(cc != NULL){
			std::vector<Config *> *children = &cc->children;
			std::vector<Config *>::iterator it;
			for(it = children->begin(); it != children->end(); it++){
				if((*it)->key == "allow"){
					const char *ip = (*it)->str();
					log_info("    allow %s", ip);
					serv->ip_filter->add_allow(ip);
				}
				if((*it)->key == "deny"){
					const char *ip = (*it)->str();
					log_info("    deny %s", ip);
					serv->ip_filter->add_deny(ip);
				}
			}
		}
	}
	
	{ // server
		const char *ip = conf.get_str("server.ip");
		int port = conf.get_num("server.port");
		if(ip == NULL || ip[0] == '\0'){
			ip = "127.0.0.1";
		}
		
		// 监听指定的ip和端口
		serv->serv_link = Link::listen(ip, port);
		if(serv->serv_link == NULL){
			log_fatal("error opening server socket! %s", strerror(errno));
			fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
			exit(1);
		}
		log_info("server listen on %s:%d", ip, port);

		std::string password;
		password = conf.get_str("server.auth");
		if(password.size() && (password.size() < 32 || password == "very-strong-password")){
			log_fatal("weak password is not allowed!");
			fprintf(stderr, "WARNING! Weak password is not allowed!\n");
			exit(1);
		}
		if(password.empty()){
			log_info("auth: off");
		}else{
			log_info("auth: on");
		}
		serv->need_auth = false;		
		if(!password.empty()){
			serv->need_auth = true;
			serv->password = password;
		}
	}
	return serv;
}

// 开始提供服务
void NetworkServer::serve(){
    // 创建写工作池和读工作池，并调用start开始工作
	writer = new ProcWorkerPool("writer");
	writer->start(num_writers);

	reader = new ProcWorkerPool("reader");
	reader->start(num_readers);

    // ready_list_t是网络连接列表
	ready_list_t ready_list;
	ready_list_t ready_list_2;
	ready_list_t::iterator it;
	const Fdevents::events_t *events;

    // 使用事件监听器开始监听各个文件描述符上的事件，将各个对象作为数据传入事件监听器中，在
    // 事件触发时将会把数据带回来，也就是把连接对象的指针或者工作池的指针带回来
    // 对于server link，也就是服务器监听的连接，我们关心数据流入的事件
	fdes->set(serv_link->fd(), FDEVENT_IN, 0, serv_link);
	// 对于读工作池和写工作池，也只关心数据流入的事件
	fdes->set(this->reader->fd(), FDEVENT_IN, 0, this->reader);
	fdes->set(this->writer->fd(), FDEVENT_IN, 0, this->writer);
	// TODO 为啥数据长度是0？
	
	uint32_t last_ticks = g_ticks;
	
	// 没有接收到退出信号的情况下，无限循环处理请求
	while(!quit){
		// status report
		// 需要汇报的时候，把连接状态写到日志里
		if((uint32_t)(g_ticks - last_ticks) >= STATUS_REPORT_TICKS){
			last_ticks = g_ticks;
			log_info("server running, links: %d", this->link_count);
		}
		
		// ready_list中存储的是需要立即处理的客户端连接，ready_list_2中存储的是
		// 在向客户端发送响应后还需要进行处理的客户端连接（也就是发送完响应后立即就有了新的请求）。
		ready_list.swap(ready_list_2);
		ready_list_2.clear();
		
		// 从事件监听器获取已经raedy的事件
		// ready_list非空说明还有一些上一次处理后产生的新的请求需要处理，这时不等待
		if(!ready_list.empty()){
			// ready_list not empty, so we should return immediately
			events = fdes->wait(0);
		}else{
			events = fdes->wait(50);
		}
		if(events == NULL){
			log_fatal("events.wait error: %s", strerror(errno));
			break;
		}
		
		// 循环处理事件
		for(int i=0; i<(int)events->size(); i++){
		    // 获取事件指针
			const Fdevent *fde = events->at(i);
			if(fde->data.ptr == serv_link){
			    // 如果是服务器连接事件
			    // 接收连接，接收连接也就是创建了一个新的服务端和客户端之间的连接，注意
			    // 将这个连接和服务器监听的连接区分开来
				Link *link = accept_link();
				if(link){
					this->link_count ++;				
					log_debug("new link from %s:%d, fd: %d, links: %d",
						link->remote_ip, link->remote_port, link->fd(), this->link_count);
					// 设置事件监听，开始监听客户端连接的数据流入的事件
					// 注意：这里没有监听数据流出的事件
	                // TODO 为啥数据长度是1？
					fdes->set(link->fd(), FDEVENT_IN, 1, link);
				}
			}else if(fde->data.ptr == this->reader || fde->data.ptr == this->writer){
			    // 如果是工作池的事件
			    // 获取工作池指针，也就是事件的数据
				ProcWorkerPool *worker = (ProcWorkerPool *)fde->data.ptr;
				ProcJob job;
				// 拿出待处理的任务
				if(worker->pop(&job) == 0){
					log_fatal("reading result from workers error!");
					exit(0);
				}
				// 处理任务
				if(proc_result(&job, &ready_list) == PROC_ERROR){
					//
				}
			}else{
			    // 其他情况下，是客户端连接的数据，也就是请求命令，处理之
			    // 在这个函数中，对于客户端连接中数据流入的情况，在这个函数中已经将网络输入的数据
			    // 读取到连接的输入缓冲区，并把ready的连接放到ready_list中，估计下面会真正的处理命令吧。
			    // 对于数据流出的情况，会将输出缓冲区的数据写到网络传输中，然后不再监听输出事件了。数据
			    // 输出的情况必定是在向客户端发送响应的时候需要监听的，整体联系起来就比较好理解了
				proc_client_event(fde, &ready_list);
			}
		}

        // 到目前位置，ready_list中存储的是已经raedy的、有输入数据的客户端连接，接下来
        // 就要进行处理了
		for(it = ready_list.begin(); it != ready_list.end(); it ++){
		    // 拿到连接
			Link *link = *it;
			if(link->error()){
				this->link_count --;
				fdes->del(link->fd());
				delete link;
				continue;
			}

            // 把输入数据封装成请求对象，在recv中会将输入缓冲区的数据读取出来，并解析成请求对象
            // 请求对象实际上就是个Bytes数组
            // 在link中也保存了请求数据，所以这的request对象没用到，直接从link里读取了
			const Request *req = link->recv();
			if(req == NULL){
				log_warn("fd: %d, link parse error, delete link", link->fd());
				this->link_count --;
				fdes->del(link->fd());
				delete link;
				continue;
			}
			// 没有输入数据，为啥还需要重新监听输入连接呢？？ TODO
			if(req->empty()){
				fdes->set(link->fd(), FDEVENT_IN, 1, link);
				continue;
			}
			
			// 记录一个时间
			link->active_time = millitime();

            // 创建一个任务
			ProcJob job;
			job.link = link;
			// 处理任务。如果是线程任务，则将任务放到工作池中。如果是直接运行的命令，则运行
			// 处理函数，并将结果发送到输出缓冲区中，等待下一次事件触发的时候处理
			this->proc(&job);
			// 如果是线程命令，没必要再监听客户端连接的事件了，将监听删除
			if(job.result == PROC_THREAD){
				fdes->del(link->fd());
				continue;
			}
			// 如果是后台运行的命令，不仅不需要再监听事件，连连接数量也减少了
			if(job.result == PROC_BACKEND){
				fdes->del(link->fd());
				this->link_count --;
				continue;
			}
			
			// 到这里只有直接运行的命令了，这时响应结果已经放到了输出缓冲区中。在这个函数中
			// 将响应发送到网络，并处理事件监听以及将新到达的请求放到ready_list_2中。
			if(proc_result(&job, &ready_list_2) == PROC_ERROR){
				//
			}
		} // end foreach ready link
	}
}

// 接收客户端的连接请求，并创建一个新的客户端连接返回。注意将客户端连接和服务端的连接区分开来
Link* NetworkServer::accept_link(){
	Link *link = serv_link->accept();
	if(link == NULL){
		log_error("accept failed! %s", strerror(errno));
		return NULL;
	}
	if(!ip_filter->check_pass(link->remote_ip)){
		log_debug("ip_filter deny link from %s:%d", link->remote_ip, link->remote_port);
		delete link;
		return NULL;
	}
				
	link->nodelay();
	link->noblock();
	link->create_time = millitime();
	link->active_time = link->create_time;
	return link;
}

// 处理待处理的任务，并将结果加入到ready_list中？不是将结果写道ready_list，而是如果写完后立即受到
// 了更多客户端请求数据，则放到ready_list中，等待下次处理。
//
// 在调用这个函数的时候任务已经处理完毕，结果已经写到客户端连接的输出缓冲区，在这个函数
// 中会把输出缓冲区的数据写到网络，并更新事件监听以及处理新到达的请求。
//
// 1. 写完后输出缓冲区非空：继续监听客户端连接的数据流出事件
// 2. 写完后输入缓冲区非空：清除对数据流入事件的监听，将客户端连接加入到ready_list中，有请求需要处理；
// 3. 写完后输入缓冲区空：继续监听客户端连接数据流入事件，等待下一次请求。
int NetworkServer::proc_result(ProcJob *job, ready_list_t *ready_list){
	Link *link = job->link;
	int len;
			
	if(job->cmd){
		job->cmd->calls += 1;
		job->cmd->time_wait += job->time_wait;
		job->cmd->time_proc += job->time_proc;
	}
	if(job->result == PROC_ERROR){
		log_info("fd: %d, proc error, delete link", link->fd());
		goto proc_err;
	}
	
	// 将输出缓冲区的数据写到网络传输，也就是把响应结果发送出去了
	len = link->write();
	//log_debug("write: %d", len);
	if(len < 0){
		log_debug("fd: %d, write: %d, delete link", link->fd(), len);
		goto proc_err;
	}

    // 输出缓冲区非空，说明没有发送完？为啥还要继续监听数据流出的事件？
    // 这时监听数据流出的事件，当socket重新变成非block、可写的状态的时候，再
    // 继续将输出缓冲区中的数据写到socket，直到全部写完。
	if(!link->output->empty()){
		fdes->set(link->fd(), FDEVENT_OUT, 1, link);
	}
	if(link->input->empty()){
	    // 输入已经为空，继续监听数据流入事件
		fdes->set(link->fd(), FDEVENT_IN, 1, link);
	}else{
	    // 否则的话，有数据流入，继续处理请求，线清除对数据流入的监听
		fdes->clr(link->fd(), FDEVENT_IN);
		ready_list->push_back(link);
	}
	return PROC_OK;

proc_err:
	this->link_count --;
	fdes->del(link->fd());
	delete link;
	return PROC_ERROR;
}

/*
event:
	read => ready_list OR close
	write => NONE
proc =>
	done: write & (read OR ready_list)
	async: stop (read & write)
	
1. When writing to a link, it may happen to be in the ready_list,
so we cannot close that link in write process, we could only
just mark it as closed.

2. When reading from a link, it is never in the ready_list, so it
is safe to close it in read process, also safe to put it into
ready_list.

3. Ignore FDEVENT_ERR

A link is in either one of these places:
	1. ready list
	2. async worker queue
So it safe to delete link when processing ready list and async worker result.
*/
// 在这个函数中处理客户端连接的数据流入和数据流出事件。数据流入的时候表示客户端发送了
// 新的请求，此时在此函数中会将请求数据读到输入缓冲区，后面将会处理请求。如果是数据流出
// 事件，说明之前没写完的socket现在可以继续写了，则在这个函数中会继续将输出缓冲区的数据
// 写到socket。
int NetworkServer::proc_client_event(const Fdevent *fde, ready_list_t *ready_list){
    // 获取事件数据，这个数据就是客户端连接的指针
	Link *link = (Link *)fde->data.ptr;
	// 如果是数据流入的事件
	if(fde->events & FDEVENT_IN){
	    // 将连接加入到ready_list中
		ready_list->push_back(link);
		if(link->error()){
			return 0;
		}
		// 将网络数据读取到输入缓冲区，输入缓冲区存储在link中
		int len = link->read();
		//log_debug("fd: %d read: %d", link->fd(), len);
		if(len <= 0){
			log_debug("fd: %d, read: %d, delete link", link->fd(), len);
			link->mark_error();
			return 0;
		}
	}
	// 如果是数据流出的事件，现在还不清除什么情况下监听了这个事件
	// 用于处理响应结果太大、一次不能完成写的情况。再proc_result中，如果
	// 发送响应没有将输出缓冲区的全部数据都写完，则会监听客户端连接的数据
	// 流出事件，直到可写的时候再继续写。
	if(fde->events & FDEVENT_OUT){
		if(link->error()){
			return 0;
		}
		// 将输出缓冲区的数据写到网络
		int len = link->write();
		if(len <= 0){
			log_debug("fd: %d, write: %d, delete link", link->fd(), len);
			link->mark_error();
			return 0;
		}
		// 如果已经写完的话，不需要在关心这个文件描述符的数据流出事件了
		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
		}
	}
	return 0;
}

// 处理任务，这时的任务是个空任务，但是其中有link指针，也就能拿到输入数据
// 在这个函数中会先根据请求数据找到处理请求的命令，再对应处理。如果是线程
// 运行的任务，则放到工作池中就返回。如果是直接运行的，则直接调用函数获取
// 到结果，并将响应结果放到客户端连接的输出缓冲区中。
void NetworkServer::proc(ProcJob *job){
	job->serv = this;
	job->result = PROC_OK;
	job->stime = millitime();

    // 获取到之前从客户端连接读取到的数据
	const Request *req = job->link->last_recv();
	Response resp;

    // 这里使用了do while(0) 来避免使用goto进行错误处理
	do{
		// AUTH
		// 处理认证请求
		if(this->need_auth && job->link->auth == false && req->at(0) != "auth"){
			resp.push_back("noauth");
			resp.push_back("authentication required");
			break;
		}
		
		// 根据输入内容获取处理的命令
		Command *cmd = proc_map.get_proc(req->at(0));
		if(!cmd){
			resp.push_back("client_error");
			resp.push_back("Unknown Command: " + req->at(0).String());
			break;
		}
		job->cmd = cmd;
		
		// 如果是在线程中运行的命令，则将命令添加的读工作池或者写工作池就可以了，
		// 接下来工作池中会去处理对应的命令
		if(cmd->flags & Command::FLAG_THREAD){
			if(cmd->flags & Command::FLAG_WRITE){
				job->result = PROC_THREAD;
				writer->push(*job);
			}else{
				job->result = PROC_THREAD;
				reader->push(*job);
			}
			return;
		}

        // 直接运行的命令，调用命令处理函数，获取到返回结果，放到resp中
		proc_t p = cmd->proc;
		job->time_wait = 1000 * (millitime() - job->stime);
		job->result = (*p)(this, job->link, *req, &resp);
		job->time_proc = 1000 * (millitime() - job->stime) - job->time_wait;
	}while(0);
	
	// 将执行结果发送出去，在send中会将数据发送到输出缓冲区，下一个时钟周期则会将数据发送到网络
	if(job->link->send(resp.resp) == -1){
		job->result = PROC_ERROR;
	}else{
		if(log_level() >= Logger::LEVEL_DEBUG){
		    // 记录日志
			log_debug("w:%.3f,p:%.3f, req: %s, resp: %s",
				job->time_wait, job->time_proc,
				serialize_req(*req).c_str(),
				serialize_req(resp.resp).c_str());
		}
	}
}


/* built-in procs */

static int proc_ping(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	return 0;
}

static int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back("ideawu's network server framework");
	resp->push_back("version");
	resp->push_back("1.0");
	resp->push_back("links");
	resp->add(net->link_count);
	{
		int64_t calls = 0;
		proc_map_t::iterator it;
		for(it=net->proc_map.begin(); it!=net->proc_map.end(); it++){
			Command *cmd = it->second;
			calls += cmd->calls;
		}
		resp->push_back("total_calls");
		resp->add(calls);
	}
	return 0;
}

static int proc_auth(NetworkServer *net, Link *link, const Request &req, Response *resp){
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		if(!net->need_auth || req[1] == net->password){
			link->auth = true;
			resp->push_back("ok");
			resp->push_back("1");
		}else{
			resp->push_back("error");
			resp->push_back("invalid password");
		}
	}
	return 0;
}
