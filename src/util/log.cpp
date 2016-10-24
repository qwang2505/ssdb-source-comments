/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "log.h"

// 全局日志对象
static Logger logger;

// 相当于初始化logger
int log_open(FILE *fp, int level, bool is_threadsafe){
	return logger.open(fp, level, is_threadsafe);
}

// 相当于初始化logger
int log_open(const char *filename, int level, bool is_threadsafe, uint64_t rotate_size){
	return logger.open(filename, level, is_threadsafe, rotate_size);
}

int log_level(){
	return logger.level();
}

void set_log_level(int level){
	logger.set_level(level);
}

// 写日志
int log_write(int level, const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(level, fmt, ap);
	va_end(ap);
	return ret;
}

/*****/
// 返回全局共享的日志对象指针
Logger* Logger::shared(){
	return &logger;
}

Logger::Logger(){
	fp = stdout;
	level_ = LEVEL_DEBUG;
	mutex = NULL;

	filename[0] = '\0';
	rotate_size_ = 0;
	stats.w_curr = 0;
	stats.w_total = 0;
}

Logger::~Logger(){
	if(mutex){
		pthread_mutex_destroy(mutex);
		free(mutex);
	}
	this->close();
}

std::string Logger::level_name(){
	switch(level_){
		case Logger::LEVEL_FATAL:
			return "fatal";
		case Logger::LEVEL_ERROR:
			return "error";
		case Logger::LEVEL_WARN:
			return "warn";
		case Logger::LEVEL_INFO:
			return "info";
		case Logger::LEVEL_DEBUG:
			return "debug";
		case Logger::LEVEL_TRACE:
			return "trace";
	}
	return "";
}

std::string Logger::output_name(){
	return filename;
}

uint64_t Logger::rotate_size(){
	return rotate_size_;
}

// 设置线程安全
void Logger::threadsafe(){
	if(mutex){
		pthread_mutex_destroy(mutex);
		free(mutex);
		mutex = NULL;
	}
	// 创建线程锁
	mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
	// 初始化线程锁
	pthread_mutex_init(mutex, NULL);
}

// 打开日志，设置日志输出的文件指针
int Logger::open(FILE *fp, int level, bool is_threadsafe){
	this->fp = fp;
	this->level_ = level;
	if(is_threadsafe){
		this->threadsafe();
	}
	return 0;
}

// 打开文件日志
int Logger::open(const char *filename, int level, bool is_threadsafe, uint64_t rotate_size){
    // 为什么减20？
	if(strlen(filename) > PATH_MAX - 20){
		fprintf(stderr, "log filename too long!");
		return -1;
	}
	this->level_ = level;
	this->rotate_size_ = rotate_size;
	strcpy(this->filename, filename);

	FILE *fp;
	if(strcmp(filename, "stdout") == 0){
		fp = stdout;
	}else if(strcmp(filename, "stderr") == 0){
		fp = stderr;
	}else{
		fp = fopen(filename, "a");
		if(fp == NULL){
			return -1;
		}

		struct stat st;
		// 获取文件状态，主要是行数，应该是用于log rotate
		int ret = fstat(fileno(fp), &st);
		if(ret == -1){
			fprintf(stderr, "fstat log file %s error!", filename);
			return -1;
		}else{
			stats.w_curr = st.st_size;
		}
	}
	return this->open(fp, level, is_threadsafe);
}

void Logger::close(){
    // 关闭日志文件
	if(fp != stdin && fp != stdout){
		fclose(fp);
	}
}

// rotate日志
void Logger::rotate(){
	fclose(fp);
	char newpath[PATH_MAX];
	time_t time;
	struct timeval tv;
	struct tm *tm;
	gettimeofday(&tv, NULL);
	time = tv.tv_sec;
	tm = localtime(&time);
	// 生成新的日志名称，按时间生成
	sprintf(newpath, "%s.%04d%02d%02d-%02d%02d%02d",
		this->filename,
		tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
		tm->tm_hour, tm->tm_min, tm->tm_sec);

    // 重命名日志文件
	//printf("rename %s => %s\n", this->filename, newpath);
	int ret = rename(this->filename, newpath);
	if(ret == -1){
		return;
	}
	// 打开新日志文件
	fp = fopen(this->filename, "a");
	if(fp == NULL){
		return;
	}
	// 重置文件行数
	stats.w_curr = 0;
}

int Logger::get_level(const char *levelname){
	if(strcmp("trace", levelname) == 0){
		return LEVEL_TRACE;
	}
	if(strcmp("debug", levelname) == 0){
		return LEVEL_DEBUG;
	}
	if(strcmp("info", levelname) == 0){
		return LEVEL_INFO;
	}
	if(strcmp("warn", levelname) == 0){
		return LEVEL_WARN;
	}
	if(strcmp("error", levelname) == 0){
		return LEVEL_ERROR;
	}
	if(strcmp("fatal", levelname) == 0){
		return LEVEL_FATAL;
	}
	if(strcmp("none", levelname) == 0){
		return LEVEL_NONE;
	}
	return LEVEL_DEBUG;
}

inline static const char* get_level_name(int level){
	switch(level){
		case Logger::LEVEL_FATAL:
			return "[FATAL] ";
		case Logger::LEVEL_ERROR:
			return "[ERROR] ";
		case Logger::LEVEL_WARN:
			return "[WARN ] ";
		case Logger::LEVEL_INFO:
			return "[INFO ] ";
		case Logger::LEVEL_DEBUG:
			return "[DEBUG] ";
		case Logger::LEVEL_TRACE:
			return "[TRACE] ";
	}
	return "";
}

#define LEVEL_NAME_LEN	8
#define LOG_BUF_LEN		4096

// 记录日志
int Logger::logv(int level, const char *fmt, va_list ap){
	if(logger.level_ < level){
		return 0;
	}

	char buf[LOG_BUF_LEN];
	int len;
	char *ptr = buf;

	time_t time;
	struct timeval tv;
	struct tm *tm;
	gettimeofday(&tv, NULL);
	time = tv.tv_sec;
	tm = localtime(&time);
	/* %3ld 在数值位数超过3位的时候不起作用, 所以这里转成int */
	// 写入日志时间
	len = sprintf(ptr, "%04d-%02d-%02d %02d:%02d:%02d.%03d ",
		tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
		tm->tm_hour, tm->tm_min, tm->tm_sec, (int)(tv.tv_usec/1000));
	if(len < 0){
		return -1;
	}
	// 移动指针
	ptr += len;

    // 写入日志等级
	memcpy(ptr, get_level_name(level), LEVEL_NAME_LEN);
	ptr += LEVEL_NAME_LEN;

    // 保留十个字符干什么？
	int space = sizeof(buf) - (ptr - buf) - 10;
	// 将具体日志信息放进来
	len = vsnprintf(ptr, space, fmt, ap);
	if(len < 0){
		return -1;
	}
	// 超出长度的部分将被忽略
	ptr += len > space? space : len;
	// 增加换行符
	*ptr++ = '\n';
	// 增加终止符
	*ptr = '\0';

    // 具体长度
	len = ptr - buf;
	// change to write(), without locking?
	// 如果线程安全，先加锁
	if(this->mutex){
		pthread_mutex_lock(this->mutex);
	}
	// 写日志文件
	fwrite(buf, len, 1, this->fp);
	fflush(this->fp);

    // 记录文件大小
	stats.w_curr += len;
	stats.w_total += len;
	// 超过rotate要求的大小，进行rotate
	if(rotate_size_ > 0 && stats.w_curr > rotate_size_){
		this->rotate();
	}
	// 释放锁
	if(this->mutex){
		pthread_mutex_unlock(this->mutex);
	}

	return len;
}

int Logger::trace(const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(Logger::LEVEL_TRACE, fmt, ap);
	va_end(ap);
	return ret;
}

int Logger::debug(const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(Logger::LEVEL_DEBUG, fmt, ap);
	va_end(ap);
	return ret;
}

int Logger::info(const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(Logger::LEVEL_INFO, fmt, ap);
	va_end(ap);
	return ret;
}

int Logger::warn(const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(Logger::LEVEL_WARN, fmt, ap);
	va_end(ap);
	return ret;
}

int Logger::error(const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(Logger::LEVEL_ERROR, fmt, ap);
	va_end(ap);
	return ret;
}

int Logger::fatal(const char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	int ret = logger.logv(Logger::LEVEL_FATAL, fmt, ap);
	va_end(ap);
	return ret;
}
