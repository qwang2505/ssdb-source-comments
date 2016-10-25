/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "bytes.h"

// 构造缓冲区，分配内存空间
Buffer::Buffer(int total){
	size_ = 0;
	total_ = origin_total = total;
	buf = (char *)malloc(total);
	data_ = buf;
}

// 删除缓冲区，释放内存空间
Buffer::~Buffer(){
	free(buf);
}

// 如果有效数据小于一般，就把有效数据搬到前面来，这样后面的空闲区域可以继续使用。
// 但是在这里没把后面的数据清空（数据已经搬到前半段了），是因为一般将缓冲区内容
// 读取到Bytes中，而Bytes不会新创造内存，使用的还是这里的内存，所以不能清空。
// Q：那等到新数据写入，原来的数据就被覆盖了，原来的Bytes指向的数据变了，怎么办？
//   一定不会发生这种情况？
void Buffer::nice(){
	// 保证不改变后半段的数据, 以便使已生成的 Bytes 不失效.
	if(data_ - buf > total_/2){
		memcpy(buf, data_, size_);
		data_ = buf;
	}
}

// 扩大缓冲区
int Buffer::grow(){ // 扩大缓冲区
	int n;
	// 如果小于8K，扩展为8K
	if(total_ < 8 * 1024){
		n = 8 * 1024;
	// 如果小于512K，扩展到原来的8倍
	}else if(total_ < 512 * 1024){
		n = 8 * total_;
	// 其他情况，扩展为原来的2倍
	}else{
		n = 2 * total_;
	}
	//log_debug("Buffer resize %d => %d", total_, n);
	// 重新申请内存空间，在这个操作里想必会把原来内存空间的数据拷贝过去
	char *p = (char *)realloc(buf, n);
	if(p == NULL){
		return -1;
	}
	// 重新设置指针以及数据大小，注意这个过程size不变
	data_ = p + (data_ - buf);
	buf = p;
	total_ = n;
	return total_;
}

// 返回分析数据，就是总大小，数据占用大小，空闲大小等
std::string Buffer::stats() const{
	char str[1024 * 32];
	str[0] = '\n';
	sprintf(str, "total: %d, data: %d, size: %d, slot: %d",
		total_, (int)(data_ - buf), size_, (int)(slot() - buf));
	return std::string(str);
}

// 从当前缓冲区读取数据到Bytes s中。缓冲区数据的格式应该是：
// |<- HEAD(max 20 bytes) ->| \n |<- BODY ->| [\r]\n
// 其中HEAD部分表示的是body数据的大小，body后的\r是可选的
int Buffer::read_record(Bytes *s){
	char *head = this->data();
	char *body = (char *)memchr(head, '\n', this->size_);
	if(body == NULL){
		return 0;
	}
	body ++;

	int head_len = body - head;
	if(head[0] < '0' || head[0] > '9'){
		return -1;
	}

	char head_str[20];
	if(head_len + 1 > (int)sizeof(head_str)){
		return -1;
	}
	memcpy(head_str, head, head_len - 1); // no '\n'
	head_str[head_len - 1] = '\0';

	int body_len = atoi(head_str);
	if(body_len < 0){
		return -1;
	}

	char *p = body + body_len;
	if(this->size_ >= head_len + body_len + 1){
		if(p[0] == '\n'){
			this->size_ -= head_len + body_len + 1;
			*s = Bytes(body, body_len);
			return 1;
		}else if(p[0] == '\r'){
			if(this->size_ >= head_len + body_len + 2){
				if(p[1] == '\n'){
					this->size_ -= head_len + body_len + 2;
					*s = Bytes(body, body_len);
					return 1;
				}else{
					return -1;
				}
			}
		}else{
			return -1;
		}
	}
	return 0;
}

// 将Bytes的数据写入到缓冲区
int Buffer::append_record(const Bytes &s){
	// 16 is the maximum length of literal string of s.size()
	int size = 16 + s.size() + 1;

	// 增加缓冲区大小
	while(size > this->space()){
		if(this->grow() == -1){
			return -1;
		}
	}

    // 把bytes的大小放到字符数组中，注意末尾加了分隔符\n
	char len[16];
	int num = snprintf(len, sizeof(len), "%d\n", (int)s.size());

    // 获取到空闲缓冲区的指针
	char *p = this->slot();
	// 拷贝表示真正数据大小的数据，这部分也就是HEAD部分
	memcpy(p, len, num);
	// 移动缓冲区指针
	p += num;

    // 拷贝真正的数据
	memcpy(p, s.data(), s.size());
	// 移动缓冲区指针
	p += s.size();

    // 设置BODY的终止标志
	*p = '\n';
	// 移动数据指针
	p += 1;
	// 重置缓冲区数据的大小
	this->size_ += (num + s.size() + 1);
	// 返回新增加的缓冲区的大小
	return (num + s.size() + 1);
}

// 将一个字符写入到缓冲区
int Buffer::append(char c){
	while(1 > this->space()){
		if(this->grow() == -1){
			return -1;
		}
	}

	*(this->slot()) = c;
	size_ += 1;
	return 1;
}

// 将一段数据写入到缓冲区
int Buffer::append(const void *p, int size){
	while(size > this->space()){
		if(this->grow() == -1){
			return -1;
		}
	}

	memcpy(this->slot(), p, size);
	size_ += size;
	return size;
}

// 将一个字符串常量的内容写入到缓冲区
int Buffer::append(const char *p){
	return this->append(p, strlen(p));
}

// 将一个Bytes写入到缓冲区
int Buffer::append(const Bytes &s){
	return this->append(s.data(), s.size());
}
