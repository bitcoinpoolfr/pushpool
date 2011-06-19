#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "autotools-config.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <jansson.h>
#include <byteswap.h>
#include <openssl/sha.h>
#include <syslog.h>
#include <pthread.h>
#include "server.h"
#include "elist.h"

#define GETWORK_HIGH 30

struct worknode {
	struct elist_head head;
	json_t *work;
};
static pthread_t getwork_thread;
static ELIST_HEAD(wl);
static pthread_cond_t wl_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t wl_mutex = PTHREAD_MUTEX_INITIALIZER;
struct getwork_private {
	CURL *curl;
	char *rpc_url;
	char *rpc_userpass;
};

static int wl_count()
{
	struct elist_head *h;
	int n = 0;
	pthread_mutex_lock(&wl_mutex);
	elist_for_each(h, &wl)
	    n++;
	pthread_mutex_unlock(&wl_mutex);
	return n;
}

static json_t *get_work(struct getwork_private *self)
{
	static int rpcid = 0;
	char s[80];
	unsigned char data[128];
	const char *data_str;
	json_t *val, *result;

	sprintf(s, "{\"method\": \"getwork\", \"params\": [], \"id\":%u}\r\n",
		rpcid++);
	/* issue JSON-RPC request */
	val = json_rpc_call(self->curl, self->rpc_url, self->rpc_userpass, s);
	if (!val)
		return NULL;
	/* decode data field, implicitly verifying 'result' is an object */
	result = json_object_get(val, "result");
	data_str = json_string_value(json_object_get(result, "data"));
	if (!data_str || !hex2bin(data, data_str, sizeof(data))) {
		json_decref(val);
		return NULL;
	}
	return val;
}

void *getwork_main(void *args)
{
	struct getwork_private *self = args;
	struct worknode *node;
	while (1) {
		while (wl_count() < GETWORK_HIGH) {
			node = malloc(sizeof(struct worknode));
			node->work = get_work(self);
			if (node->work == NULL) {
				free(node);
				continue;
			}
			pthread_mutex_lock(&wl_mutex);
			elist_add_tail(&node->head, &wl);
			pthread_mutex_unlock(&wl_mutex);
		}
		pthread_mutex_lock(&wl_mutex);
		pthread_cond_wait(&wl_cond, &wl_mutex);
		pthread_mutex_unlock(&wl_mutex);
	}
	return NULL;
}

void getwork_finish(void)
{
	pthread_cancel(getwork_thread);
	pthread_join(getwork_thread, NULL);
}

bool getwork_init(void)
{
	struct getwork_private *args = malloc(sizeof(struct getwork_private));
	args->curl = curl_easy_init();
	if (!args->curl) {
		applog(LOG_ERR, "curl init failed.");
		free(args);
		return false;
	}
	args->rpc_url = strdup(srv.rpc_url);
	args->rpc_userpass = strdup(srv.rpc_userpass);
	if (pthread_create(&getwork_thread, NULL, getwork_main, args)) {
		applog(LOG_ERR, "Failed to create getwork thread.");
		free(args->rpc_userpass);
		free(args->rpc_url);
		curl_easy_cleanup(args->curl);
		free(args);
		return false;
	}
	return true;
}

json_t *next_getwork()
{
	int try = 1;
	struct worknode *node, *iter;
	json_t *ret = NULL;
	while (!wl_count() && try++ <= 3) {
		usleep(250000);	/* hopefully gets filled. */
	}
	pthread_mutex_lock(&wl_mutex);
	elist_for_each_entry_safe(node, iter, &wl, head) {
		elist_del(&node->head);
		ret = node->work;
		free(node);
		pthread_cond_signal(&wl_cond);
		break;
	}
	pthread_mutex_unlock(&wl_mutex);
	if (ret == NULL)
		applog(LOG_WARNING, "next_getwork: no getworks in cache.");
	return ret;
}

void getwork_flush()
{
	struct worknode *node, *iter;
	pthread_mutex_lock(&wl_mutex);
	elist_for_each_entry_safe(node, iter, &wl, head) {
		elist_del(&node->head);
		json_decref(node->work);
		free(node);
	}
	pthread_mutex_unlock(&wl_mutex);
	pthread_cond_signal(&wl_cond);
}
