/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "net/fde.h"
#include "util/log.h"
#include "slave.h"
#include "include.h"

// 初始化。在serv.h中会进行初始化
Slave::Slave(SSDB *ssdb, SSDB *meta, const char *ip, int port, bool is_mirror){
	thread_quit = false;
	// 设置数据库对象指针
	this->ssdb = ssdb;
	this->meta = meta;
	// 设置状态
	this->status = DISCONNECTED;
	// 设置master的ip和端口。注意：初始化中传递进来的是master的ip和端口信息
	this->master_ip = std::string(ip);
	this->master_port = port;
	// 设置是否mirror，需要看这个属性的用法
	this->is_mirror = is_mirror;
	// 通过是否mirror来定义操作日志的类型
	if(this->is_mirror){
		this->log_type = BinlogType::MIRROR;
	}else{
		this->log_type = BinlogType::SYNC;
	}
	
	// 设置slave的id为master的ip+端口。这些信息是存储在slave里的，多个master肯定
	// 不会重复
	{
		char buf[128];
		snprintf(buf, sizeof(buf), "%s|%d", master_ip.c_str(), master_port);
		this->set_id(buf);
	}
	
	this->link = NULL;
	this->last_seq = 0;
	this->last_key = "";
	this->connect_retry = 0;
	
	this->copy_count = 0;
	this->sync_count = 0;
}

// 销毁slave对象
Slave::~Slave(){
	log_debug("stopping slave thread...");
	// 如果线程没有推出，退出线程
	if(!thread_quit){
		stop();
	}
	if(link){
		delete link;
	}
	log_debug("Slave finalized");
}

// slave的状态
std::string Slave::stats() const{
	std::string s;
	s.append("slaveof " + master_ip + ":" + str(master_port) + "\n");
	s.append("    id         : " + id_ + "\n");
	if(is_mirror){
		s.append("    type       : mirror\n");
	}else{
		s.append("    type       : sync\n");
	}

	s.append("    status     : ");
	switch(status){
	case DISCONNECTED:
		s.append("DISCONNECTED\n");
		break;
	case INIT:
		s.append("INIT\n");
		break;
	case COPY:
		s.append("COPY\n");
		break;
	case SYNC:
		s.append("SYNC\n");
		break;
	}

	s.append("    last_seq   : " + str(last_seq) + "\n");
	s.append("    copy_count : " + str(copy_count) + "\n");
	s.append("    sync_count : " + str(sync_count) + "");
	return s;
}

// 开始运行slave，在这里会创建新线程
void Slave::start(){
    // 这里应该是为了兼容旧版本的slave状态信息，以旧的格式来加在slave状态
	migrate_old_status();
	// 从数据库里加在slave的信息
	load_status();
	log_debug("last_seq: %" PRIu64 ", last_key: %s",
		last_seq, hexmem(last_key.data(), last_key.size()).c_str());

	thread_quit = false;
	// 创建线程，开始运行
	int err = pthread_create(&run_thread_tid, NULL, &Slave::_run_thread, this);
	if(err != 0){
		log_error("can't create thread: %s", strerror(err));
	}
}

// 停止线程运行
void Slave::stop(){
    // 设置推出变量的值
	thread_quit = true;
	void *tret;
	// join表示等线程运行完毕再退出
	int err = pthread_join(run_thread_tid, &tret);
    if(err != 0){
		log_error("can't join thread: %s", strerror(err));
	}
}

// 设置slave的id，这里的id用的是master的id加上port生成的
void Slave::set_id(const std::string &id){
	this->id_ = id;
}

// 迁移老状态，不知道是什么意思
// 下面的status_key()返回的是存储slave信息的key，这个是版本升级所作的兼容
void Slave::migrate_old_status(){
    // 从metadb中获取老的slave信息，存储的是最后一个序列号的信息加上最后一个key的信息
	std::string old_key = "new.slave.status|" + this->id_;
	std::string val;
	int old_found = meta->raw_get(old_key, &val);
	if(!old_found){
		return;
	}
	if(val.size() < sizeof(uint64_t)){
		log_error("invalid format of status");
		return;
	}
	// 解析最后一个最小的序列号
	last_seq = *((uint64_t *)(val.data()));
	// 设置最后一个key
	last_key.assign(val.data() + sizeof(uint64_t), val.size() - sizeof(uint64_t));
	// migrate old status
	log_info("migrate old version slave status to new format, last_seq: %" PRIu64 ", last_key: %s",
		last_seq, hexmem(last_key.data(), last_key.size()).c_str());
	
	save_status();
	// 删除old key
	if(meta->raw_del(old_key) == -1){
		log_fatal("meta db error!");
		exit(1);
	}
}

// 在slave中存储信息的key，是hset中存储的信息
std::string Slave::status_key(){
	static std::string key;
	if(key.empty()){
		key = "slave.status." + this->id_;
	}
	return key;
}

// 从数据库load slave的信息，last_key和last_seq
void Slave::load_status(){
	std::string key;
	std::string seq;
	meta->hget(status_key(), "last_key", &key);
	meta->hget(status_key(), "last_seq", &seq);
	if(!key.empty()){
		this->last_key = key;
	}
	if(!seq.empty()){
		this->last_seq = str_to_uint64(seq);
	}
}

// 保存新的slave状态信息
void Slave::save_status(){
    // 存储last_key和last_seq
	std::string seq = str(this->last_seq);
	meta->hset(status_key(), "last_key", this->last_key);
	meta->hset(status_key(), "last_seq", seq);
}

// 连接master
int Slave::connect(){
    // 获取到ip和端口
	const char *ip = this->master_ip.c_str();
	int port = this->master_port;
	
	// 不会每次调用都重连，而是每隔50次重连一次
	if(++connect_retry % 50 == 1){
		log_info("[%s][%d] connecting to master at %s:%d...", this->id_.c_str(), (connect_retry-1)/50, ip, port);
		// 连接master，创建link
		link = Link::connect(ip, port);
		if(link == NULL){
			log_error("[%s]failed to connect to master: %s:%d!", this->id_.c_str(), ip, port);
			goto err;
		}else{
			status = INIT;
			// 重值尝试连接次数
			connect_retry = 0;
			const char *type = is_mirror? "mirror" : "sync";
			
			// 设置认证
			if(!this->auth.empty()){
				const std::vector<Bytes> *resp;
				resp = link->request("auth", this->auth);
				if(resp->empty() || resp->at(0) != "ok"){
					log_error("auth error");
					delete link;
					link = NULL;
					sleep(1);
					return -1;
				}
			}
			
			// 发送sync请求
			// 在这里会带上slave的type，也就是同步的类型，是sync还是mirror
			// sync表示主从结构，mirror表示多主结构
			link->send("sync140", str(this->last_seq), this->last_key, type);
			if(link->flush() == -1){
				log_error("[%s] network error", this->id_.c_str());
				delete link;
				link = NULL;
				goto err;
			}
			log_info("[%s] ready to receive binlogs", this->id_.c_str());
			return 1;
		}
	}
	return 0;
err:
	return -1;
}

// 开始运行，尚不知道在这个里面做了些什么事情
void* Slave::_run_thread(void *arg){
    // 拿到slave对象
	Slave *slave = (Slave *)arg;
	const std::vector<Bytes> *req;
	Fdevents select;
	const Fdevents::events_t *events;
	int idle = 0;
	bool reconnect = false;
	
	// 宏还可以在函数中间定义？
#define RECV_TIMEOUT		200
#define MAX_RECV_TIMEOUT	300 * 1000
#define MAX_RECV_IDLE		MAX_RECV_TIMEOUT/RECV_TIMEOUT

    // 主循环开始运行，通过变量检测是否退出
	while(!slave->thread_quit){
	    // 需要重连，删除老连接，重置状态
		if(reconnect){
			slave->status = DISCONNECTED;
			reconnect = false;
			// 不再监听文件描述符的事件
			select.del(slave->link->fd());
			// 删除连接
			delete slave->link;
			slave->link = NULL;
			// 1秒后重新连接
			sleep(1);
		}
		// 尚未连接，则进行连接
		if(!slave->connected()){
		    // 连接master
			if(slave->connect() != 1){
				usleep(100 * 1000);
			}else{
			    // 如果成功，设置监听文件描述符的数据流入事件
				select.set(slave->link->fd(), FDEVENT_IN, 0, NULL);
			}
			continue;
		}
		
		// 等待事件
		events = select.wait(RECV_TIMEOUT);
		if(events == NULL){
			log_error("events.wait error: %s", strerror(errno));
			sleep(1);
			continue;
		}else if(events->empty()){
		    // 超过最多等待时间，重新连接master
			if(idle++ >= MAX_RECV_IDLE){
				log_error("the master hasn't responsed for awhile, reconnect...");
				idle = 0;
				reconnect = true;
			}
			continue;
		}
		idle = 0;

        // 从网络读取数据到输入缓冲区
		if(slave->link->read() <= 0){
			log_error("link.read error: %s, reconnecting to master", strerror(errno));
			reconnect = true;
			continue;
		}

        // 如果有数据，一直读取，直到所有输入都读取完
		while(1){
		    // 读取数据
			req = slave->link->recv();
			if(req == NULL){
				log_error("link.recv error: %s, reconnecting to master", strerror(errno));
				reconnect = true;
				break;
			}else if(req->empty()){
				break;
			}else if(req->at(0) == "noauth"){
				log_error("authentication required");
				reconnect = true;
				sleep(1);
				break;
			}else{
			    // 处理接收到的master的数据
				if(slave->proc(*req) == -1){
					goto err;
				}
			}
		}
	} // end while
	log_info("Slave thread quit");
	return (void *)NULL;

err:
	log_fatal("Slave thread exit unexpectedly");
	exit(0);
	return (void *)NULL;;
}

// 处理master返回的数据
int Slave::proc(const std::vector<Bytes> &req){
    // 将请求加在到操作日志
	Binlog log;
	if(log.load(req[0]) == -1){
		log_error("invalid binlog!");
		return 0;
	}
	// 这里终于用上这个同步类型了
	// 但是下面貌似也没有用到这个东西，只是记录了日志。。。
	const char *sync_type = this->is_mirror? "mirror" : "sync";
	// 根据操作日志类型进行不同的处理
	switch(log.type()){
		case BinlogType::NOOP:
			return this->proc_noop(log, req);
			break;
		// master拷贝数据
		case BinlogType::COPY:{
		    // 表示开始从master拷贝数据
		    // 将slave的状态设置为拷贝数据中
			status = COPY;
			if(req.size() >= 2){
				log_debug("[%s] %s [%d]", sync_type, log.dumps().c_str(), req[1].size());
			}else{
				log_debug("[%s] %s", sync_type, log.dumps().c_str());
			}
			this->proc_copy(log, req);
			break;
		}
		case BinlogType::SYNC:
		case BinlogType::MIRROR:{
		    // 设置状态为同步中
			status = SYNC;
			if(++sync_count % 1000 == 1){
				log_info("sync_count: %" PRIu64 ", last_seq: %" PRIu64 ", seq: %" PRIu64 "",
					sync_count, this->last_seq, log.seq());
			}
			if(req.size() >= 2){
				log_debug("[%s] %s [%d]", sync_type, log.dumps().c_str(), req[1].size());
			}else{
				log_debug("[%s] %s", sync_type, log.dumps().c_str());
			}
			// 处理操作日志
			this->proc_sync(log, req);
			break;
		}
		default:
			break;
	}
	return 0;
}

// 没有任何操作，只是修改操作日志的序列号
// 当长时间没有操作，或者在mirror模式下长时间没有操作时，master会向salve发送这个消息，
// 类似与heartbeat的操作
int Slave::proc_noop(const Binlog &log, const std::vector<Bytes> &req){
	uint64_t seq = log.seq();
	if(this->last_seq != seq){
		log_debug("noop last_seq: %" PRIu64 ", seq: %" PRIu64 "", this->last_seq, seq);
		this->last_seq = seq;
		this->save_status();
	}
	return 0;
}

// 处理拷贝信息
int Slave::proc_copy(const Binlog &log, const std::vector<Bytes> &req){
	switch(log.cmd()){
	    // 开始拷贝，不需要作任何事情
		case BinlogCommand::BEGIN:
			log_info("copy begin");
			break;
		// 结束拷贝，说明基本数据已经拷贝完
		case BinlogCommand::END:
			log_info("copy end, copy_count: %" PRIu64 ", last_seq: %" PRIu64 ", seq: %" PRIu64,
				copy_count, this->last_seq, log.seq());
			// 设置状态为同步，表示从现在开始同步数据
			this->status = SYNC;
			// TODO 为啥设置last_key为空？
			this->last_key = "";
			// 保存新的状态
			this->save_status();
			break;
		// 默认情况，是一条待拷贝的操作日志
		default:
			if(++copy_count % 1000 == 1){
			    // 记录部分日志
				log_info("copy_count: %" PRIu64 ", last_seq: %" PRIu64 ", seq: %" PRIu64 "",
					copy_count, this->last_seq, log.seq());
			}
			// 处理拷贝过程重的操作日志
			return proc_sync(log, req);
			break;
	}
	return 0;
}

// 处理同步信息，请求是一条操作日志
int Slave::proc_sync(const Binlog &log, const std::vector<Bytes> &req){
    // 根据不同的命令进行不同的操作
	switch(log.cmd()){
	    // SET命令
		case BinlogCommand::KSET:
			{
				if(req.size() != 2){
					break;
				}
				std::string key;
				if(decode_kv_key(log.key(), &key) == -1){
					break;
				}
				log_trace("set %s", hexmem(key.data(), key.size()).c_str());
				if(ssdb->set(key, req[1], log_type) == -1){
					return -1;
				}
			}
			break;
		// DEL命令
		case BinlogCommand::KDEL:
			{
				std::string key;
				if(decode_kv_key(log.key(), &key) == -1){
					break;
				}
				log_trace("del %s", hexmem(key.data(), key.size()).c_str());
				if(ssdb->del(key, log_type) == -1){
					return -1;
				}
			}
			break;
		// HSET命令
		case BinlogCommand::HSET:
			{
				if(req.size() != 2){
					break;
				}
				std::string name, key;
				if(decode_hash_key(log.key(), &name, &key) == -1){
					break;
				}
				log_trace("hset %s %s",
					hexmem(name.data(), name.size()).c_str(),
					hexmem(key.data(), key.size()).c_str());
				if(ssdb->hset(name, key, req[1], log_type) == -1){
					return -1;
				}
			}
			break;
		// HDEL命令
		case BinlogCommand::HDEL:
			{
				std::string name, key;
				if(decode_hash_key(log.key(), &name, &key) == -1){
					break;
				}
				log_trace("hdel %s %s",
					hexmem(name.data(), name.size()).c_str(),
					hexmem(key.data(), key.size()).c_str());
				if(ssdb->hdel(name, key, log_type) == -1){
					return -1;
				}
			}
			break;
	    // ZSET命令
		case BinlogCommand::ZSET:
			{
				if(req.size() != 2){
					break;
				}
				std::string name, key;
				if(decode_zset_key(log.key(), &name, &key) == -1){
					break;
				}
				log_trace("zset %s %s",
					hexmem(name.data(), name.size()).c_str(),
					hexmem(key.data(), key.size()).c_str());
				if(ssdb->zset(name, key, req[1], log_type) == -1){
					return -1;
				}
			}
			break;
		// ZDEL命令
		case BinlogCommand::ZDEL:
			{
				std::string name, key;
				if(decode_zset_key(log.key(), &name, &key) == -1){
					break;
				}
				log_trace("zdel %s %s",
					hexmem(name.data(), name.size()).c_str(),
					hexmem(key.data(), key.size()).c_str());
				if(ssdb->zdel(name, key, log_type) == -1){
					return -1;
				}
			}
			break;
		// QSET/QPUSH_BACK/QPUSH_FRONT命令
		case BinlogCommand::QSET:
		case BinlogCommand::QPUSH_BACK:
		case BinlogCommand::QPUSH_FRONT:
			{
				if(req.size() != 2){
					break;
				}
				std::string name;
				uint64_t seq;
				if(decode_qitem_key(log.key(), &name, &seq) == -1){
					break;
				}
				if(seq < QITEM_MIN_SEQ || seq > QITEM_MAX_SEQ){
					break;
				}
				int ret;
				if(log.cmd() == BinlogCommand::QSET){
					log_trace("qset %s %" PRIu64 "", hexmem(name.data(), name.size()).c_str(), seq);
					ret = ssdb->qset_by_seq(name, seq, req[1], log_type);
				}else if(log.cmd() == BinlogCommand::QPUSH_BACK){
					log_trace("qpush_back %s", hexmem(name.data(), name.size()).c_str());
					ret = ssdb->qpush_back(name, req[1], log_type);
				}else{
					log_trace("qpush_front %s", hexmem(name.data(), name.size()).c_str());
					ret = ssdb->qpush_front(name, req[1], log_type);
				}
				if(ret == -1){
					return -1;
				}
			}
			break;
		// QPOP_BACK/QPOP_FRONT命令
		case BinlogCommand::QPOP_BACK:
		case BinlogCommand::QPOP_FRONT:
			{
				int ret;
				const Bytes name = log.key();
				std::string tmp;
				if(log.cmd() == BinlogCommand::QPOP_BACK){
					log_trace("qpop_back %s", hexmem(name.data(), name.size()).c_str());
					ret = ssdb->qpop_back(name, &tmp, log_type);
				}else{
					log_trace("qpop_front %s", hexmem(name.data(), name.size()).c_str());
					ret = ssdb->qpop_front(name, &tmp, log_type);
				}
				if(ret == -1){
					return -1;
				}
			}
			break;
		default:
			log_error("unknown binlog, type=%d, cmd=%d", log.type(), log.cmd());
			break;
	}
	// 更新序列号
	this->last_seq = log.seq();
	if(log.type() == BinlogType::COPY){
	    // 更新key
		this->last_key = log.key().String();
	}
	// 存储状态
	this->save_status();
	return 0;
}

