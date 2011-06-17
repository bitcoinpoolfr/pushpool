
/*
 * Copyright 2011 Shane Wegner
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file COPYING.  If not, write to
 * the Free Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "autotools-config.h"

#ifdef HAVE_POSTGRESQL

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <pthread.h>
#include <libpq-fe.h>

#include "server.h"

#define DEFAULT_STMT_PWDB \
	"SELECT password FROM pool_worker WHERE username = $1"
#define DEFAULT_STMT_SHARELOG \
	"insert into shares (rem_host, username, our_result, \
	upstream_result, reason, solution) values($1, $2, $3, $4, $5, decode($6, 'hex'))"

static pthread_t shr_thread;
struct thread_args {
	void *conn;
	char *stmt;
};
struct logdata {
	struct elist_head head;
	time_t time;
	int n;
	char **values;
};
static ELIST_HEAD(lq);
static pthread_cond_t lq_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t lq_mutex = PTHREAD_MUTEX_INITIALIZER;

static bool pg_conncheck(void *conn)
{
	if (PQstatus(conn) != CONNECTION_OK) {
		applog(LOG_WARNING,
		       "Connection to PostgreSQL lost: reconnecting.");
		PQreset(conn);
		if (PQstatus(conn) != CONNECTION_OK) {
			applog(LOG_ERR, "Reconnect attempt failed.");
			return false;
		}
	}
	return true;
}

static char *pg_pwdb_lookup(const char *user)
{
	char *pw = NULL;
	PGresult *res;
	const char *paramvalues[] = { user };
	if (!pg_conncheck(srv.db_cxn))
		return NULL;
	res =
	    PQexecParams(srv.db_cxn, srv.db_stmt_pwdb, 1, NULL,
			 paramvalues, NULL, NULL, 0);
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		applog(LOG_ERR, "pg_pwdb_lookup query failed: %s",
		       PQerrorMessage(srv.db_cxn));
		goto out;
	}
	if (PQnfields(res) != 1 || PQntuples(res) < 1)
		goto out;
	pw = strdup(PQgetvalue(res, 0, 0));
 out:
	PQclear(res);
	return pw;
}

static bool pg_sharelog(const char *rem_host, const char *username,
			const char *our_result,
			const char *upstream_result, const char *reason,
			const char *solution)
{
	const char *paramvalues[] = { rem_host, username, our_result,
		upstream_result, reason, solution
	};
	int i;
	time_t t;
	struct logdata *share = malloc(sizeof(struct logdata));
	INIT_ELIST_HEAD(&share->head);
	share->n = 6;
	share->values = calloc(sizeof(share->values), share->n);
	for (i = 0; i < share->n; i++)
		share->values[i] = (paramvalues[i] != NULL) ?
		    strdup(paramvalues[i]) : NULL;
	time(&t);
	share->time = t;
	pthread_mutex_lock(&lq_mutex);
	elist_add_tail(&share->head, &lq);
	elist_for_each_entry(share, &lq, head) {
		if (share->time + 30 < t)
			applog(LOG_WARNING,
			       "Oldest share in sharelog queue is %d seconds old.",
			       t - share->time);
		break;
	}
	pthread_cond_signal(&lq_cond);
	pthread_mutex_unlock(&lq_mutex);
	return true;
}

static void pg_close(void)
{
	PQfinish(srv.db_cxn);
	pthread_cancel(shr_thread);
	pthread_join(shr_thread, NULL);
}

void sharecleanup(void *args)
{
	struct thread_args *self = args;
	PQfinish(self->conn);
	free(self->stmt);
	free(self);
	applog(LOG_NOTICE, "sharelog thread exited cleanly.");
}

void *sharemain(void *args)
{
	int i;
	int oldstate;
	struct thread_args *self = args;
	struct logdata *share, *iter;
	PGresult *res;
	pthread_cleanup_push(sharecleanup, args);
	while (1) {
		pthread_mutex_lock(&lq_mutex);
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &oldstate);
		pthread_cond_wait(&lq_cond, &lq_mutex);
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &oldstate);
		elist_for_each_entry_safe(share, iter, &lq, head) {
			if (!pg_conncheck(self->conn))
				break;
			elist_del(&share->head);
			pthread_mutex_unlock(&lq_mutex);
			res =
			    PQexecParams(self->conn, self->stmt, share->n, NULL,
					 (const char *const *)share->values,
					 NULL, NULL, 0);
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
				applog(LOG_ERR, "pg_sharelog failed: %s",
				       PQerrorMessage(self->conn));
			PQclear(res);
			for (i = 0; i < share->n; i++)
				free(share->values[i]);
			free(share->values);
			free(share);
			pthread_mutex_lock(&lq_mutex);
		}
		pthread_mutex_unlock(&lq_mutex);
	}
	pthread_cleanup_pop(1);
	pthread_exit(NULL);
}

static bool pg_open(void)
{
	struct thread_args *shr_args = malloc(sizeof(struct thread_args));
	char *portstr = NULL;
	if (srv.db_port > 0)
		if (asprintf(&portstr, "%d", srv.db_port) < 0)
			return false;
	shr_args->conn = PQsetdbLogin(srv.db_host, portstr, NULL, NULL,
				      srv.db_name, srv.db_username,
				      srv.db_password);
	srv.db_cxn = PQsetdbLogin(srv.db_host, portstr, NULL, NULL,
				  srv.db_name, srv.db_username,
				  srv.db_password);
	free(portstr);
	if (PQstatus(srv.db_cxn) != CONNECTION_OK ||
	    PQstatus(shr_args->conn) != CONNECTION_OK) {
		applog(LOG_ERR, "failed to connect to postgresql: %s",
		       PQerrorMessage(srv.db_cxn));
		PQfinish(srv.db_cxn);
		PQfinish(shr_args->conn);
		free(shr_args);
		return false;
	}
	if (srv.db_stmt_pwdb == NULL || !*srv.db_stmt_pwdb)
		srv.db_stmt_pwdb = strdup(DEFAULT_STMT_PWDB);
	if (srv.db_stmt_sharelog == NULL || !*srv.db_stmt_sharelog)
		srv.db_stmt_sharelog = strdup(DEFAULT_STMT_SHARELOG);
	shr_args->stmt = strdup(srv.db_stmt_sharelog);
	if (pthread_create(&shr_thread, NULL, sharemain, shr_args)) {
		applog(LOG_ERR, "Failed to create sharelog thread.");
		PQfinish(srv.db_cxn);
		PQfinish(shr_args->conn);
		free(shr_args);
		return false;
	}
	return true;
}

struct server_db_ops postgresql_db_ops = {
	.pwdb_lookup = pg_pwdb_lookup,
	.sharelog = pg_sharelog,
	.open = pg_open,
	.close = pg_close,
};

#endif
