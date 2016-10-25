/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef UTIL_BYTES_H_
#define UTIL_BYTES_H_

#include "strings.h"

// readonly
// to replace std::string
// 只读的字符串，替代string的对象
//
// Bytes本身是不创造内存空间的，必须再初始化的时候传入数据指针
// 也就是说Bytes本身不进行内存管理，必须由外部操作。一般是从Buffer读取的吧。
class Bytes{
	private:
	    // 具体数据
		const char *data_;
		// 数据大小
		int size_;
	public:
	    // 初始化
		Bytes(){
			data_ = "";
			size_ = 0;
		}

        // 有数据的初始化
		Bytes(void *data, int size){
			data_ = (char *)data;
			size_ = size;
		}

		Bytes(const char *data, int size){
			data_ = data;
			size_ = size;
		}

        // 用string初始化
		Bytes(const std::string &str){
			data_ = str.data();
			size_ = (int)str.size();
		}

        // char*初始化。因为是const char*，所以这里知道长度？
        // strlen是什么鬼？就是求const char*的大小
		Bytes(const char *str){
			data_ = str;
			size_ = (int)strlen(str);
		}

        // 返回数据指针
		const char* data() const{
			return data_;
		}

		bool empty() const{
			return size_ == 0;
		}

		int size() const{
			return size_;
		}

        // 比较两个Bytes对象。先比较数据，再比较长度
		int compare(const Bytes &b) const{
			const int min_len = (size_ < b.size_) ? size_ : b.size_;
			int r = memcmp(data_, b.data_, min_len);
			if(r == 0){
				if (size_ < b.size_) r = -1;
				else if (size_ > b.size_) r = +1;
			}
			return r;
		}

        // 返回string
		std::string String() const{
			return std::string(data_, size_);
		}

        // 数据转换为int
		int Int() const{
			return str_to_int(data_, size_);
		}

        // 数据转换为int64
		int64_t Int64() const{
			return str_to_int64(data_, size_);
		}

		uint64_t Uint64() const{
			return str_to_uint64(data_, size_);
		}

		double Double() const{
			return str_to_double(data_, size_);
		}
};

// 重载等号操作符
inline
bool operator==(const Bytes &x, const Bytes &y){
	return ((x.size() == y.size()) &&
			(memcmp(x.data(), y.data(), x.size()) == 0));
}

// 重载不等号操作符
inline
bool operator!=(const Bytes &x, const Bytes &y){
	return !(x == y);
}

// 重载大于号操作符
inline
bool operator>(const Bytes &x, const Bytes &y){
	return x.compare(y) > 0;
}

// 重载大于等于号操作符
inline
bool operator>=(const Bytes &x, const Bytes &y){
	return x.compare(y) >= 0;
}

// 重载小于号操作符
inline
bool operator<(const Bytes &x, const Bytes &y){
	return x.compare(y) < 0;
}

// 重载小于等于号操作符
inline
bool operator<=(const Bytes &x, const Bytes &y){
	return x.compare(y) <= 0;
}


// 定义缓冲区，这是程序的缓冲区，注意和网络socket的缓冲区区分开来
class Buffer{
	private:
	    // buf和data有啥区别？各自是用来干吗的？
	    //
	    // buf指向整个缓冲区的内存区域，data指向有效数据的内存区域。随着数据被读取，data会
	    // 改变（向后移），但是buf不变，一直指向整个缓冲区的开始位置
		char *buf;
		char *data_;
		// 缓冲区中数据的大小
		int size_;
		// 缓冲区中总的大小
		int total_;
		// 原来的大小，可能是缓冲区大小变化的时候用的吧
		int origin_total;
	public:
	    // 指定尺寸，构造缓冲区
		Buffer(int total);
		~Buffer();

        // 总的缓冲区大小
		int total() const{ // 缓冲区大小
			return total_;
		}

		bool empty() const{
			return size_ == 0;
		}

		// 指向数据的指针
		char* data() const{
			return data_;
		}

        // 缓冲区中数据的大小
		int size() const{
			return size_;
		}

        // 缓冲区中空闲地址
        // 直接这么返回不会有问题吗？万一超出怎么半？
		char* slot() const{
			return data_ + size_;
		}

        // 缓冲区中空闲大小
        // 这用到buf了，但是不知道是干吗用的
		int space() const{
			return total_ - (int)(data_ - buf) - size_;
		}

        // 增加数据大小，说明写入了数据
        // 此时不用改变data指针，因为新数据是放到老数据后面的，数据的头指针没变
		void incr(int num){
			size_ += num;
		}

        // 减少数据大小，这时需要改变头指针，因为头部数据被读取了
		void decr(int num){
			size_ -= num;
			data_ += num;
		}

		// 保证不改变后半段的数据, 以便使已生成的 Bytes 不失效.
		void nice();
		// 扩大缓冲区
		int grow();

		std::string stats() const;
		// 从缓冲区读取数据，放到Bytes中
		int read_record(Bytes *s);

        // 下面这一组函数是将指定数据写入到缓冲区
		int append(char c);
		int append(const char *p);
		int append(const void *p, int size);
		int append(const Bytes &s);

		int append_record(const Bytes &s);
};


class Decoder{
private:
	const char *p;
	int size;
	Decoder(){}
public:
	Decoder(const char *p, int size){
		this->p = p;
		this->size = size;
	}
	int skip(int n){
		if(size < n){
			return -1;
		}
		p += n;
		size -= n;
		return n;
	}
	int read_int64(int64_t *ret){
		if(size_t(size) < sizeof(int64_t)){
			return -1;
		}
		if(ret){
			*ret = *(int64_t *)p;
		}
		p += sizeof(int64_t);
		size -= sizeof(int64_t);
		return sizeof(int64_t);
	}
	int read_uint64(uint64_t *ret){
		if(size_t(size) < sizeof(uint64_t)){
			return -1;
		}
		if(ret){
			*ret = *(uint64_t *)p;
		}
		p += sizeof(uint64_t);
		size -= sizeof(uint64_t);
		return sizeof(uint64_t);
	}
	int read_data(std::string *ret){
		int n = size;
		if(ret){
			ret->assign(p, size);
		}
		p += size;
		size = 0;
		return n;
	}
	int read_8_data(std::string *ret=NULL){
		if(size < 1){
			return -1;
		}
		int len = (uint8_t)p[0];
		p += 1;
		size -= 1;
		if(size < len){
			return -1;
		}
		if(ret){
			ret->assign(p, len);
		}
		p += len;
		size -= len;
		return 1 + len;
	}
};

#endif

