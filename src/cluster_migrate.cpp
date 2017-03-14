#include "cluster_migrate.h"
#include "util/log.h"
#include "SSDB_client.h"

#define BATCH_SIZE 100

// 初始化客户端连接，将返回一个客户端指针
static ssdb::Client* init_client(const std::string &ip, int port){
    // 根据IP和端口连接客户端
	ssdb::Client *client = ssdb::Client::connect(ip, port);
	if(client == NULL){
		return NULL;
	}

	const std::vector<std::string>* resp;
	// 连接客户端之后发送了一个请求，这是在干什么？
	// 发送了一个请求，设置了客户端的ignore_key_range属性的值，不知道在哪里用到
	resp = client->request("ignore_key_range");
	if(!resp || resp->empty() || resp->at(0) != "ok"){
		log_error("src server ignore_key_range error!");
		delete client;
		return NULL;
	}
	return client;
}

ClusterMigrate::ClusterMigrate(){
	src = NULL;
	dst = NULL;
}

ClusterMigrate::~ClusterMigrate(){
	delete src;
	delete dst;
}

// 检查服务器版本是否支持迁移
int ClusterMigrate::check_version(ssdb::Client *client){
	const std::vector<std::string>* resp;
	resp = client->request("version");
	if(!resp || resp->size() < 2 || resp->at(0) != "ok"){
		log_error("ssdb-server 1.9.0 or higher is required!");
		return -1;
	}
	return 0;
}

// 将指定key的数据从一个节点移动到另一个节点，返回移动的数据的大小
int ClusterMigrate::move_key(const std::string &key){
	std::string val;
	ssdb::Status s;
	s = src->get(key, &val);
	if(s.not_found()){
		return 0;
	}
	if(!s.ok()){
		log_error("src server error! %s", s.code().c_str());
		return -1;
	}
	// TODO: TTL
	s = dst->set(key, val);
	if(!s.ok()){
		log_error("dst server error! %s", s.code().c_str());
		return -1;
	}
	s = src->del(key);
	if(!s.ok()){
		log_error("src server error! %s", s.code().c_str());
		return -1;
	}
	return (int)key.size() + (int)val.size();
}

// 将src节点中的前num_keys个数据记录移动到dst节点中。moved_max_key返回移动的最大的key，也就是移动后dst的最后一个key
// 注意：要保证各个节点的区间不重合，迁移需要保证dst的key区间一定小于src的key区间。
// 举例：
//      src: (100, 200], dst: (0, 100]
// 迁移50个key。迁移后：
//      src: (150, 200], dst: (0, 150]
//
// 注意：参数max_key表示的是src的end
int64_t ClusterMigrate::move_range(const std::string &max_key, std::string *moved_max_key, int num_keys){
	// get key range
	std::vector<std::string> keys;
	ssdb::Status s;
	// 从 "" 开始遍历, 是因为在中断之后, 重新执行时, 之前被中断了的数据是可以被迁移的. 
	s = src->keys("", max_key, num_keys, &keys);
	if(!s.ok()){
		log_error("response error: %s", s.code().c_str());
		return -1;
	}
	if(keys.empty()){
		return 0;
	}

    // 移动的数据的最大的key
	*moved_max_key = keys[keys.size() - 1];
	
	// src的新key区间应该从移动后的最大的key开始
	KeyRange new_src_range(*moved_max_key, max_key);
	log_info("new src: %s", new_src_range.str().c_str());
	// 修改src的key区间，这里修改的是src节点上的数据
	s = src->set_kv_range(*moved_max_key, max_key);
	if(!s.ok()){
		log_error("src server set_kv_range error! %s", s.code().c_str());
		return -1;
	}

    // 真正开始迁移数据，bytes记录移动的数据的总大小
	int64_t bytes = 0;
	while(1){
		// move key range
		for(int i=0; i<(int)keys.size(); i++){
			const std::string &key = keys[i];
			// 移动单条数据
			int ret = move_key(key);
			if(ret == -1){
				log_error("move key %s error! %s", key.c_str(), s.code().c_str());
				return -1;  
			}
			bytes += ret;
		}
		
		// keys已经全部移动完，清理
		keys.clear();

        // 确保要迁移的数据区间没有剩余的数据，如果还有，继续迁移，知道指定区间的数据
        // 都迁移完为止
		s = src->keys("", *moved_max_key, num_keys, &keys);
		if(!s.ok()){
			log_error("response error: %s", s.code().c_str());
			return -1;
		}
		if(keys.empty()){
			break;
		}
	}
	return bytes;
}

// 从一个节点向另一个节点迁移指定数量的KV数据
// 将src中的前num_keys条数据迁移到dst中
int64_t ClusterMigrate::migrate_kv_data(Node *src_node, Node *dst_node, int num_keys){
    // 连接两个节点
	src = init_client(src_node->ip, src_node->port);
	if(src == NULL){
		log_error("failed to connect to server!");
		return -1;
	}
	dst = init_client(dst_node->ip, dst_node->port);
	if(dst == NULL){
		log_error("failed to connect to server!");
		return -1;
	}
	
	// 检查版本支持
	if(check_version(src) == -1){
		return -1;
	}
	if(check_version(dst) == -1){
		return -1;
	}
	
	ssdb::Status s;
	// 原来两个节点各自的key区间
	KeyRange src_range = src_node->range;
	KeyRange dst_range = dst_node->range;
	
	log_info("old src %s", src_range.str().c_str());
	log_info("old dst %s", dst_range.str().c_str());

	std::string moved_max_key;
	int64_t bytes;
	// 将src的前num_keys条数据迁移到dst中
	bytes = move_range(src_range.end, &moved_max_key, num_keys);
	if(bytes == -1){
		return -1;
	}
	if(bytes == 0){
		return 0;
	}

	// update key range
	// 更新src节点的range，这里更新的是运行迁移的节点的信息
	src_node->range = KeyRange(moved_max_key, src_range.end);
	{
	    // 更新目标节点的key区间信息
		dst_node->range = KeyRange(dst_range.begin, moved_max_key);
		log_info("new dst: %s", dst_node->range.str().c_str());
		ssdb::Status s = dst->set_kv_range(dst_node->range.begin, dst_node->range.end);
		if(!s.ok()){
			log_fatal("dst server set_kv_range error!");
			return -1;
		}
	}
	return bytes;
}
