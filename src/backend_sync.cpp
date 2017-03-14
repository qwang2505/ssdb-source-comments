/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include <string>
#include "backend_sync.h"
#include "util/log.h"
#include "util/strings.h"

// 初始化，传递一个ssdb的实例，用于操作数据库
BackendSync::BackendSync(SSDBImpl *ssdb, int sync_speed){
	thread_quit = false;
	this->ssdb = ssdb;
	// 指定同步的速度，还不知道怎么用
	this->sync_speed = sync_speed;
}

// 销毁同步，这时候肯定server退出了
BackendSync::~BackendSync(){
	thread_quit = true;
	int retry = 0;
	int MAX_RETRY = 100;
	while(retry++ < MAX_RETRY){
		// there is something wrong that sleep makes other threads
		// unable to acquire the mutex
		{
			Locking l(&mutex);
			if(workers.empty()){
				break;
			}
		}
		usleep(50 * 1000);
	}
	if(retry >= MAX_RETRY){
		log_info("Backend worker not exit expectedly");
	}
	log_debug("BackendSync finalized");
}

// 返回同步的状态
std::vector<std::string> BackendSync::stats(){
	std::vector<std::string> ret;
	// 遍历线程id和客户端指针
	std::map<pthread_t, Client *>::iterator it;

	Locking l(&mutex);
	for(it = workers.begin(); it != workers.end(); it++){
		Client *client = it->second;
		// 获取各个客户端的同步状态
		ret.push_back(client->stats());
	}
	return ret;
}

// 处理sync140的请求，在新线程中进行处理
void BackendSync::proc(const Link *link){
	log_info("fd: %d, accept sync client", link->fd());
	struct run_arg *arg = new run_arg();
	arg->link = link;
	arg->backend = this;

	pthread_t tid;
	int err = pthread_create(&tid, NULL, &BackendSync::_run_thread, arg);
	if(err != 0){
		log_error("can't create thread: %s", strerror(err));
		delete link;
	}
}

// 真正处理请求
void* BackendSync::_run_thread(void *arg){
    // 从参数中解析出必要的信息
	struct run_arg *p = (struct run_arg*)arg;
	BackendSync *backend = (BackendSync *)p->backend;
	Link *link = (Link *)p->link;
	delete p;

	// set Link non block
	// 设置成阻塞
	link->noblock(false);

    // 拿到操作日志
	SSDBImpl *ssdb = (SSDBImpl *)backend->ssdb;
	BinlogQueue *logs = ssdb->binlogs;

    // 创建一个新的客户端，实际上就是一个slave
	Client client(backend);
	client.link = link;
	// 初始化客户端，会从请求中读取last_key, last_seq，并设置客户端同步的状态
	// COPY表示数据拷贝过程，当新的slave或者同步超出操作日志的范围时，需要进行拷贝操作，先将已有数据
	// 全部拷贝至slave，然后再开始同步。
	// SYNC表示同步操作，同步通过操作日志队列来完成。操作日志队列的大小是有限的。如果同步操作没跟上操作的速度，
	// 也就是出现OUT_OF_SYNC，这时候需要重新先进行COPY操作，然后才能继续同步。
	client.init();

    // 将客户端添加到workers中
	{
		pthread_t tid = pthread_self();
		Locking l(&backend->mutex);
		backend->workers[tid] = &client;
	}

// sleep longer to reduce logs.find
#define TICK_INTERVAL_MS	300
#define NOOP_IDLES			(3000/TICK_INTERVAL_MS)

	int idle = 0;
	// 主循环开始同步数据
	while(!backend->thread_quit){
		// TODO: test
		//usleep(2000 * 1000);
		
		// 同步有断层，重置客户端，进入COPY状态，先将数据全部拷贝到salve再
		// 进行同步
		if(client.status == Client::OUT_OF_SYNC){
			client.reset();
			continue;
		}
		
		bool is_empty = true;
		// WARN: MUST do first sync() before first copy(), because
		// sync() will refresh last_seq, and copy() will not
		// 如果处于拷贝阶段，那么sync函数将会更新last_seq为最新的操作日志序列号，并返回0
		// 如果处于同步阶段，sync函数会将一条操作日志放到输出缓冲区，并返回1
		// 也就是说，在同步的时候每次只同步一条操作日志
		if(client.sync(logs)){
			is_empty = false;
		}
		// 如果处于数据拷贝阶段，调用copy函数进行数据拷贝。在copy函数中会将尽可能多的、不超过
		// 2M、3000条操作日志的数据放到输出缓冲区中。如果拷贝完毕，会更新状态。
		if(client.status == Client::COPY){
			if(client.copy()){
				is_empty = false;
			}
		}
		// 如果结果是空的，说明没有同步日志。这时候隔一定时间要发送一条空的操作日志，是为了heartbeat？
		if(is_empty){
			if(idle >= NOOP_IDLES){
				idle = 0;
				client.noop();
			}else{
				idle ++;
				usleep(TICK_INTERVAL_MS * 1000);
			}
		// 否则，有操作日志被同步，重置计数器
		}else{
			idle = 0;
		}

        // 待同步的数据有多少M
		float data_size_mb = link->output->size() / 1024.0 / 1024.0;
		// 将数据发送出去
		if(link->flush() == -1){
			log_info("%s:%d fd: %d, send error: %s", link->remote_ip, link->remote_port, link->fd(), strerror(errno));
			break;
		}
		// 调整sleep的时间，以控制同步的速度。很显然sync_speed的单位是M/s
		if(backend->sync_speed > 0){
		    // usleep的单位是微秒
		    // sleep一定的时间，以便在指定的传输速度下要发送的数据可以完成发送。
			usleep((data_size_mb / backend->sync_speed) * 1000 * 1000);
		}
	}

    // 同步主循环退出，可能是发送数据失败，与slave的连接断开等，退出线程。
	log_info("Sync Client quit, %s:%d fd: %d, delete link", link->remote_ip, link->remote_port, link->fd());
	delete link;

	Locking l(&backend->mutex);
	backend->workers.erase(pthread_self());
	return (void *)NULL;
}


/* Client */

// 表示同步过程中的一个客户端，实际上就是一个slave
BackendSync::Client::Client(const BackendSync *backend){
    // 默认状态是初始化
	status = Client::INIT;
	this->backend = backend;
	link = NULL;
	last_seq = 0;
	last_noop_seq = 0;
	last_key = "";
	is_mirror = false;
	iter = NULL;
}

// 销毁对象
BackendSync::Client::~Client(){
	if(iter){
		delete iter;
		iter = NULL;
	}
}

// 获取slave同步的状态信息
std::string BackendSync::Client::stats(){
	std::string s;
	s.append("client " + str(link->remote_ip) + ":" + str(link->remote_port) + "\n");
	s.append("    type     : ");
	if(is_mirror){
		s.append("mirror\n");
	}else{
		s.append("sync\n");
	}
	
	s.append("    status   : ");
	switch(status){
	case INIT:
		s.append("INIT\n");
		break;
	case OUT_OF_SYNC:
		s.append("OUT_OF_SYNC\n");
		break;
	case COPY:
		s.append("COPY\n");
		break;
	case SYNC:
		s.append("SYNC\n");
		break;
	}
	
	s.append("    last_seq : " + str(last_seq) + "");
	return s;
}

// 初始化客户端，也就是slave。在这个函数中会根据last_key和last_seq来
// 判断处于COPY状态还是SYNC状态
void BackendSync::Client::init(){
    // 拿到请求
	const std::vector<Bytes> *req = this->link->last_recv();
	last_seq = 0;
	// 解析出最后序列号和最后的key，需要看看这两个分别是干什么的
	if(req->size() > 1){
		last_seq = req->at(1).Uint64();
	}
	last_key = "";
	if(req->size() > 2){
		last_key = req->at(2).String();
	}
	// is_mirror
	// 指定是否镜像
	if(req->size() > 3){
		if(req->at(3).String() == "mirror"){
			is_mirror = true;
		}
	}
	const char *type = is_mirror? "mirror" : "sync";
	// last_key用于再COPY过程中记录上一次拷贝到哪个key，last_seq用于记录上一次同步
	// 的序列号是什么。如果last_key等于空而last_seq不为0，说明处于SYNC阶段
	if(last_key == "" && last_seq != 0){
		log_info("[%s] %s:%d fd: %d, sync, seq: %" PRIu64 ", key: '%s'",
			type,
			link->remote_ip, link->remote_port,
			link->fd(),
			last_seq, hexmem(last_key.data(), last_key.size()).c_str()
			);
		// 设置状态为同步
		this->status = Client::SYNC;
		
		// 发送一条操作日志，表示拷贝已经结束，接下来进入同步阶段
		Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::END, "");
		log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
		link->send(log.repr(), "copy_end");
	// 其他条件下，说明处于COPY阶段
	}else{
		// a slave must reset its last_key when receiving 'copy_end' command
		log_info("[%s] %s:%d fd: %d, copy recover, seq: %" PRIu64 ", key: '%s'",
			type,
			link->remote_ip, link->remote_port,
			link->fd(),
			last_seq, hexmem(last_key.data(), last_key.size()).c_str()
			);
		// 设置状态为拷贝数据阶段
		this->status = Client::COPY;
	}
}

// 重置同步相关的数据。发生这种情况可能是同步有断层，这时候需要进行COPY操作
void BackendSync::Client::reset(){
	log_info("%s:%d fd: %d, copy begin", link->remote_ip, link->remote_port, link->fd());
	// 设置状态为copy
	this->status = Client::COPY;
	// 从头开始进行copy操作
	this->last_seq = 0;
	this->last_key = "";

	Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::BEGIN, "");
	log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
	// 发送开始拷贝的操作日志
	link->send(log.repr(), "copy_begin");
}

// 不需要进行操作
// 在两种情况下需要发送空操作：
// 1. 主从结构，在一定时间内没有数据同步时；
// 2. 主主结构，由于不会同步操作日志，因此每隔一段时间发送一次
void BackendSync::Client::noop(){
	uint64_t seq;
	if(this->status == Client::COPY && this->last_key.empty()){
		seq = 0;
	}else{
		seq = this->last_seq;
		this->last_noop_seq = this->last_seq;
	}
	Binlog noop(seq, BinlogType::NOOP, BinlogCommand::NONE, "");
	//log_debug("fd: %d, %s", link->fd(), noop.dumps().c_str());
	link->send(noop.repr());
}

// 从master拷贝数据到slave。在拷贝的过程中会根据last_key的值来进行增量的拷贝。
// 拷贝不会一次性把所有数据都拷贝，否则可能block太久的时间引起超时。
// 拷贝过程没有处理type=mirror的情况？不用处理。拷贝时的操作日志是根据数据库中的
// 数据生成的，不包含从其他节点拷贝数据时的操作日志，因此都是需要拷贝到另外一个
// 节点的。
int BackendSync::Client::copy(){
    // 创建迭代器来遍历获取到需要拷贝的数据
	if(this->iter == NULL){
		log_info("new iterator, last_key: '%s'", hexmem(last_key.data(), last_key.size()).c_str());
		std::string key = this->last_key;
		// 如果没有定义last_key，从头开始遍历所有数据
		if(this->last_key.empty()){
			key.push_back(DataType::MIN_PREFIX);
		}
		this->iter = backend->ssdb->iterator(key, "", -1);
		log_info("iterator created, last_key: '%s'", hexmem(last_key.data(), last_key.size()).c_str());
	}
	int ret = 0;
	int iterate_count = 0;
	int64_t stime = time_ms();
	// 主循环开始拷贝数据
	while(true){
		// Prevent copy() from blocking too long
		// 防止copy操作阻塞太久，迭代1000次或者输出内容的长度达到2M时，停止数据拷贝操作
		if(++iterate_count > 1000 || link->output->size() > 2 * 1024 * 1024){
			break;
		}
		// 阻塞时间太长，停止拷贝操作
		if(time_ms() - stime > 3000){
			log_info("copy blocks too long, flush");
			break;
		}
		
		// 拷贝结束
		if(!iter->next()){
			goto copy_end;
		}
		Bytes key = iter->key();
		if(key.size() == 0){
			continue;
		}
		// finish copying all valid data types
		// 所有合法的数据已经拷贝完
		if(key.data()[0] > DataType::MAX_PREFIX){
			goto copy_end;
		}
		// 获取到value
		Bytes val = iter->val();
		// 设置last_key，这个last_key貌似是用于在拷贝过程中记录上次拷贝位置的，需要再看
		this->last_key = key.String();

		// 根据数据类型确定命令类型
		char cmd = 0;
		char data_type = key.data()[0];
		if(data_type == DataType::KV){
			cmd = BinlogCommand::KSET;
		}else if(data_type == DataType::HASH){
			cmd = BinlogCommand::HSET;
		}else if(data_type == DataType::ZSET){
			cmd = BinlogCommand::ZSET;
		}else if(data_type == DataType::QUEUE){
			cmd = BinlogCommand::QPUSH_BACK;
		}else{
		    // 未知的数据类型，不拷贝
			continue;
		}
		
		ret = 1;
		
		// 根据命令创建操作日志
		Binlog log(this->last_seq, BinlogType::COPY, cmd, slice(key));
		log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
		// 将操作日志发送给客户端，也就是slave节点
		link->send(log.repr(), val);
	}
	return ret;

copy_end:		
	log_info("%s:%d fd: %d, copy end", link->remote_ip, link->remote_port, link->fd());
	// 拷贝完成，将状态设置为同步
	this->status = Client::SYNC;
	// 删除迭代器
	delete this->iter;
	this->iter = NULL;

    // 向slave发送一条操作日志，表明拷贝过程已经结束
	Binlog log(this->last_seq, BinlogType::COPY, BinlogCommand::END, "");
	log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
	link->send(log.repr(), "copy_end");
	return 1;
}

// 从master向slave同步数据，参数是master的操作日志，是个循环队列
// 每次执行此函数，将拷贝一条操作日志到slave
//
// 如果当前处于COPY阶段，那么执行此函数会将last_seq更新成最新的操作日志序列号，
// 且会删除用于拷贝的迭代器，以便再COPY中重新创建迭代器获取到新写入的数据，并
// 返回结果0.
int BackendSync::Client::sync(BinlogQueue *logs){
	Binlog log;
	while(1){
		int ret = 0;
		// 期望从哪个序列号开始拷贝
		uint64_t expect_seq = this->last_seq + 1;
		// 如果是处于COPY状态，找到leveldb中存储的最后一条操作日志
		// 这是因为接下来在COPY的过程中会把在这之前的操作都拷贝到slave上，
		// 然后接着的同步过程，从这个序列号开始即可
		if(this->status == Client::COPY && this->last_seq == 0){
			ret = logs->find_last(&log);
		}else{
		    // 指定了序列号，找到下一条需要拷贝的日志
			ret = logs->find_next(expect_seq, &log);
		}
		if(ret == 0){
			return 0;
		}
		// 状态是COPY，并且last_key和log的key之间有差距，说明拷贝还没有开始
		// 或者还没有拷贝完毕，需要继续拷贝数据
		// 条件中还有一个重点就是日志的key在游标的后面，所以才不用拷贝。如果
		// 时在游标的前面，在下面会把操作日志同步到slave上
		if(this->status == Client::COPY && log.key() > this->last_key){
			log_debug("fd: %d, last_key: '%s', drop: %s",
				link->fd(),
				hexmem(this->last_key.data(), this->last_key.size()).c_str(),
				log.dumps().c_str());
			// 设置序列号，等拷贝结束之后，从这个序列号开始同步数据
			this->last_seq = log.seq();
			// WARN: When there are writes behind last_key, we MUST create
			// a new iterator, because iterator will not know this key.
			// Because iterator ONLY iterates throught keys written before
			// iterator is created.
			// 这里删除迭代器，是因为在上一次拷贝之后可能有新值写入，所以重新创建
			// 一个迭代器，否则新写入的数据不会被拷贝
			if(this->iter){
				delete this->iter;
				this->iter = NULL;
			}
			continue;
		}
		// 没找到想要拷贝的操作日志，说明操作日志之间有断层，需要先进行拷贝再同步，设置状态
		// 为OUT_OF_SYNC，接下来的处理中会重新开始拷贝
		if(this->last_seq != 0 && log.seq() != expect_seq){
			log_warn("%s:%d fd: %d, OUT_OF_SYNC! log.seq: %" PRIu64 ", expect_seq: %" PRIu64 "",
				link->remote_ip, link->remote_port,
				link->fd(),
				log.seq(),
				expect_seq
				);
			this->status = Client::OUT_OF_SYNC;
			return 1;
		}
	
		// update last_seq
		// 更新上一条拷贝的数据
		this->last_seq = log.seq();

        // 操作日志的类型为镜像的话，不需要将日志发送给slave，而是发送空日志
        // 日志的类型为mirror，表示master设置了type=mirror。如果this->is_mirror表示slave
        // 也设置了type=mirror。此时是多主结构而不是主从结构。多主结构的意思是：每个节点
        // 都是主节点构成的集群，意味着集群内的服务器互为主从，互相同步用户的操作。日志类型
        // 为mirror，表示这条操作日志是从其他节点同步数据产生的操作日志，不需要同步到其他的
        // 节点，否则可能导致一条日志在节点之间被不断循环
		char type = log.type();
		if(type == BinlogType::MIRROR && this->is_mirror){
			if(this->last_seq - this->last_noop_seq >= 1000){
				this->noop();
				return 1;
			}else{
				continue;
			}
		}
		
		// 到这里找到了一条待拷贝的操作日志，退出循环，接下来同步到slave
		break;
	}

    // 将操作日志发送到slave。如果是更新的日志，需要传递更新后的值。如果是
    // 删除命令，不需要传递值。
	int ret = 0;
	std::string val;
	switch(log.cmd()){
		case BinlogCommand::KSET:
		case BinlogCommand::HSET:
		case BinlogCommand::ZSET:
		case BinlogCommand::QSET:
		case BinlogCommand::QPUSH_BACK:
		case BinlogCommand::QPUSH_FRONT:
			ret = backend->ssdb->raw_get(log.key(), &val);
			if(ret == -1){
				log_error("fd: %d, raw_get error!", link->fd());
			}else if(ret == 0){
				//log_debug("%s", hexmem(log.key().data(), log.key().size()).c_str());
				log_trace("fd: %d, skip not found: %s", link->fd(), log.dumps().c_str());
			}else{
				log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
				link->send(log.repr(), val);
			}
			break;
		case BinlogCommand::KDEL:
		case BinlogCommand::HDEL:
		case BinlogCommand::ZDEL:
		case BinlogCommand::QPOP_BACK:
		case BinlogCommand::QPOP_FRONT:
			log_trace("fd: %d, %s", link->fd(), log.dumps().c_str());
			link->send(log.repr());
			break;
	}
	return 1;
}
