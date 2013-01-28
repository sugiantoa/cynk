/****************************************************************************
 * Cynk
 * Copyright (C) 2011  Nan Dun <dun@logos.ic.i.u-tokyo.ac.jp>
 * Contributor : Sugianto Angkasa <sugianto@logos.ic.i.u-tokyo.ac.jp>
 * The University of Tokyo
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * This program can be distributed under the terms of the GNU GPL.
 * See the file COPYING.
 ***************************************************************************/

/*
 * cynk.c
 * Sync utility for Cloud storage
 */

#define _GNU_SOURCE /* avoid implicit declaration of *pt* functions */
#include "config.h"

#ifdef linux
#define _XOPEN_SOURCE 500 /* for pread()/pwrite() */
#endif

#include <fuse.h>
#include <fuse_opt.h>
#include <fuse_lowlevel.h>
#include <ulockmgr.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <semaphore.h>
#include <pthread.h>
#include <netdb.h>
#include <signal.h>
#include <pwd.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/utsname.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

#include <glib.h>
#include "cache.h"

#if GLIB_CHECK_VERSION(2, 16, 0)
#define GLIB_HASH_TABLE_HAS_ITER
#endif

#define MAX_BUF_LEN  	1024
#define TEMP_STR_LEN	7
#define REPO_HOME		".cynk"
#define TRANS_HOME		".cynk/transfer"
#define SESSION_HOME	"/tmp"


#ifndef MAP_LOCKED
#define MAP_LOCKED 0
#endif

#if !defined(MAP_ANONYMOUS) && defined(MAP_ANON)
#define MAP_ANONYMOUS MAP_ANON
#endif


#if FUSE_VERSION >= 23
#define SSHFS_USE_INIT
#endif

#define SSH_FXP_INIT                1
#define SSH_FXP_VERSION             2
#define SSH_FXP_OPEN                3
#define SSH_FXP_CLOSE               4
#define SSH_FXP_READ                5
#define SSH_FXP_WRITE               6
#define SSH_FXP_LSTAT               7
#define SSH_FXP_FSTAT               8
#define SSH_FXP_SETSTAT             9
#define SSH_FXP_FSETSTAT           10
#define SSH_FXP_OPENDIR            11
#define SSH_FXP_READDIR            12
#define SSH_FXP_REMOVE             13
#define SSH_FXP_MKDIR              14
#define SSH_FXP_RMDIR              15
#define SSH_FXP_REALPATH           16
#define SSH_FXP_STAT               17
#define SSH_FXP_RENAME             18
#define SSH_FXP_READLINK           19
#define SSH_FXP_SYMLINK            20
#define SSH_FXP_STATUS            101
#define SSH_FXP_HANDLE            102
#define SSH_FXP_DATA              103
#define SSH_FXP_NAME              104
#define SSH_FXP_ATTRS             105
#define SSH_FXP_EXTENDED          200
#define SSH_FXP_EXTENDED_REPLY    201

#define SSH_FILEXFER_ATTR_SIZE          0x00000001
#define SSH_FILEXFER_ATTR_UIDGID        0x00000002
#define SSH_FILEXFER_ATTR_PERMISSIONS   0x00000004
#define SSH_FILEXFER_ATTR_ACMODTIME     0x00000008
#define SSH_FILEXFER_ATTR_EXTENDED      0x80000000

#define SSH_FX_OK                            0
#define SSH_FX_EOF                           1
#define SSH_FX_NO_SUCH_FILE                  2
#define SSH_FX_PERMISSION_DENIED             3
#define SSH_FX_FAILURE                       4
#define SSH_FX_BAD_MESSAGE                   5
#define SSH_FX_NO_CONNECTION                 6
#define SSH_FX_CONNECTION_LOST               7
#define SSH_FX_OP_UNSUPPORTED                8

#define SSH_FXF_READ            0x00000001
#define SSH_FXF_WRITE           0x00000002
#define SSH_FXF_APPEND          0x00000004
#define SSH_FXF_CREAT           0x00000008
#define SSH_FXF_TRUNC           0x00000010
#define SSH_FXF_EXCL            0x00000020

/* statvfs@openssh.com f_flag flags */
#define SSH2_FXE_STATVFS_ST_RDONLY	0x00000001
#define SSH2_FXE_STATVFS_ST_NOSUID	0x00000002

#define SFTP_EXT_POSIX_RENAME "posix-rename@openssh.com"
#define SFTP_EXT_STATVFS "statvfs@openssh.com"

#define PROTO_VERSION 3

#define MY_EOF 1

#define MAX_REPLY_LEN (1 << 17)

#define RENAME_TEMP_CHARS 8

#define SFTP_SERVER_PATH "/usr/lib/sftp-server"

#define SSHNODELAY_SO "sshnodelay.so"

struct buffer {
	uint8_t *p;
	size_t len;
	size_t size;
};

struct list_head {
	struct list_head *prev;
	struct list_head *next;
};

struct request;
typedef void (*request_func)(struct request *);

struct request {
	unsigned int want_reply;
	sem_t ready;
	uint8_t reply_type;
	int replied;
	int error;
	struct buffer reply;
	struct timeval start;
	void *data;
	request_func end_func;
	size_t len;
	struct list_head list;
};

struct read_chunk {
	sem_t ready;
	off_t offset;
	size_t size;
	struct buffer data;
	int refs;
	int res;
	long modifver;
};

struct sshfs_file {
	struct buffer handle;
	struct list_head write_reqs;
	pthread_cond_t write_finished;
	int write_error;
	struct read_chunk *readahead;
	off_t next_pos;
	int is_seq;
	int connver;
	int modifver;
	int refs;
	int online;
};

struct cynk {
	char *local;
	char *remote;
	char *mountpoint;
	char *session;
	char *default_session;
	int mode;

	GHashTable *born;
	GHashTable *dead;
	pthread_mutex_t born_lock;
	pthread_mutex_t dead_lock;
	char *repo_root;
	char *trans_root;
	char *exclude_file;
	char *include_file;
	char *rsync_common_opts;

	int sync_in_progress;
	pthread_mutex_t sync_lock;
	
	int block;
	pthread_cond_t block_cond;
	pthread_mutex_t block_lock;
	
	int duration;
	int window;
	struct timespec sysc_stamp;
	
	int rsync_on;
	int sshfs_on;
	char *online_dir;
	size_t len_online_dir;
	
	int sync_thread_stop;
	pthread_t sync_thread;
	pthread_attr_t sync_thread_attr;
	pthread_mutex_t sync_thread_stop_lock;
	pthread_cond_t sync_thread_stop_cond;
	
	int cmd;
	char *cmd_sockpath;
	int cmd_sock;
	pthread_t cmd_thread;
	pthread_attr_t cmd_thread_attr;
	int cmd_started;

	char *progname;
	char *username;
	char *userhome;
	pid_t pid;
	uid_t uid;
	gid_t gid;
	
	int debug;
	int foreground;
	int dryrun;
	int verbosity;
	int verbose_cnt;

	/* SSHFS stuff */
	char *directport;
	char *ssh_command;
	char *sftp_server;
	struct fuse_args ssh_args;
	char *workarounds;
	int rename_workaround;
	int nodelay_workaround;
	int nodelaysrv_workaround;
	int truncate_workaround;
	int buflimit_workaround;
	int transform_symlinks;
	int follow_symlinks;
	int no_check_root;
	int detect_uid;
	unsigned max_read;
	unsigned max_write;
	unsigned ssh_ver;
	int sync_write;
	int sync_read;
	int reconnect;
	char *host;
	char *base_path;
	GHashTable *reqtab;
	pthread_mutex_t lock;
	pthread_mutex_t lock_write;
	int processing_thread_started;
	unsigned int randseed;
	int fd;
	int ptyfd;
	int ptyslavefd;
	int connver;
	int server_version;
	unsigned remote_uid;
	unsigned local_uid;
	int remote_uid_detected;
	unsigned blksize;
	long modifver;
	unsigned outstanding_len;
	unsigned max_outstanding_len;
	pthread_cond_t outstanding_cond;
	int password_stdin;
	char *password;
	int ext_posix_rename;
	int ext_statvfs;

	/* statistics */
	uint64_t bytes_sent;
	uint64_t bytes_received;
	uint64_t num_sent;
	uint64_t num_received;
	unsigned int min_rtt;
	unsigned int max_rtt;
	uint64_t total_rtt;
	unsigned int num_connect;
	/*end*/
};

static struct cynk cynk;

enum {
	KEY_HELP,
	KEY_HELP_FULL,
	KEY_VERSION,
	KEY_FOREGROUND,
	KEY_DEBUG,
	KEY_DEBUG_ALL,
	KEY_DRYRUN,
	KEY_CMD_SYNC,
	KEY_MODE_MANUAL,
	KEY_DURATION,
	KEY_WINDOW,
	KEY_BLOCK_SYSC,
	
	KEY_PORT,
	KEY_COMPRESS,
	KEY_CONFIGFILE,
	KEY_CMD_SSHFS,
	KEY_ONLINE_DIR,
	KEY_DISABLE_RSYNC,
};

static const char *ssh_opts[] = {
	"AddressFamily",
	"BatchMode",
	"BindAddress",
	"ChallengeResponseAuthentication",
	"CheckHostIP",
	"Cipher",
	"Ciphers",
	"Compression",
	"CompressionLevel",
	"ConnectionAttempts",
	"ConnectTimeout",
	"ControlMaster",
	"ControlPath",
	"GlobalKnownHostsFile",
	"GSSAPIAuthentication",
	"GSSAPIDelegateCredentials",
	"HostbasedAuthentication",
	"HostKeyAlgorithms",
	"HostKeyAlias",
	"HostName",
	"IdentitiesOnly",
	"IdentityFile",
	"KbdInteractiveAuthentication",
	"KbdInteractiveDevices",
	"LocalCommand",
	"LogLevel",
	"MACs",
	"NoHostAuthenticationForLocalhost",
	"NumberOfPasswordPrompts",
	"PasswordAuthentication",
	"Port",
	"PreferredAuthentications",
	"ProxyCommand",
	"PubkeyAuthentication",
	"RekeyLimit",
	"RhostsRSAAuthentication",
	"RSAAuthentication",
	"ServerAliveCountMax",
	"ServerAliveInterval",
	"SmartcardDevice",
	"StrictHostKeyChecking",
	"TCPKeepAlive",
	"UsePrivilegedPort",
	"UserKnownHostsFile",
	"VerifyHostKeyDNS",
	NULL,
};


#define CYNK_OPT(t, p, v) { t, offsetof(struct cynk, p), v }
#define SSHFS_OPT(t, p, v) { t, offsetof(struct cynk, p), v }

static struct fuse_opt cynk_opts[] = {
	CYNK_OPT("verbosity=%d",		verbosity, 0),
	
	/* Append a space if the option takes an argument */
	FUSE_OPT_KEY("-V",              KEY_VERSION),
	FUSE_OPT_KEY("--version",       KEY_VERSION),
	FUSE_OPT_KEY("-h",              KEY_HELP),
	FUSE_OPT_KEY("-H",              KEY_HELP_FULL),
	FUSE_OPT_KEY("--help",          KEY_HELP_FULL),
	FUSE_OPT_KEY("-f",              KEY_FOREGROUND),
	FUSE_OPT_KEY("-d", 				KEY_DEBUG),
	FUSE_OPT_KEY("-D",				KEY_DEBUG_ALL),
	FUSE_OPT_KEY("-n",				KEY_DRYRUN),
	FUSE_OPT_KEY("--dry-run", 		KEY_DRYRUN),
	FUSE_OPT_KEY("-S",				KEY_CMD_SYNC),
	FUSE_OPT_KEY("-m",				KEY_MODE_MANUAL),
	FUSE_OPT_KEY("-t ",				KEY_DURATION),
	FUSE_OPT_KEY("-w ",				KEY_WINDOW),
	FUSE_OPT_KEY("-B",				KEY_BLOCK_SYSC),
	FUSE_OPT_KEY("-x ",				KEY_ONLINE_DIR),
	FUSE_OPT_KEY("-z",				KEY_DISABLE_RSYNC),
	FUSE_OPT_KEY("-o",				KEY_CMD_SSHFS),	

	SSHFS_OPT("directport=%s",     directport, 0),
	SSHFS_OPT("ssh_command=%s",    ssh_command, 0),
	SSHFS_OPT("sftp_server=%s",    sftp_server, 0),
	SSHFS_OPT("max_read=%u",       max_read, 0),
	SSHFS_OPT("max_write=%u",      max_write, 0),
	SSHFS_OPT("ssh_protocol=%u",   ssh_ver, 0),
	SSHFS_OPT("-1",                ssh_ver, 1),
	SSHFS_OPT("workaround=%s",     workarounds, 0),
	SSHFS_OPT("idmap=none",        detect_uid, 0),
	SSHFS_OPT("idmap=user",        detect_uid, 1),
	SSHFS_OPT("sshfs_sync",        sync_write, 1),
	SSHFS_OPT("no_readahead",      sync_read, 1),
	SSHFS_OPT("reconnect",         reconnect, 1),
	SSHFS_OPT("transform_symlinks", transform_symlinks, 1),
	SSHFS_OPT("follow_symlinks",   follow_symlinks, 1),
	SSHFS_OPT("no_check_root",     no_check_root, 1),
	SSHFS_OPT("password_stdin",    password_stdin, 1),

	FUSE_OPT_KEY("-p ",            KEY_PORT),
	FUSE_OPT_KEY("-C",             KEY_COMPRESS),
	FUSE_OPT_KEY("-F ",            KEY_CONFIGFILE),
	FUSE_OPT_END
};

static struct fuse_opt workaround_opts[] = {
	SSHFS_OPT("none",       rename_workaround, 0),
	SSHFS_OPT("none",       nodelay_workaround, 0),
	SSHFS_OPT("none",       nodelaysrv_workaround, 0),
	SSHFS_OPT("none",       truncate_workaround, 0),
	SSHFS_OPT("none",       buflimit_workaround, 0),
	SSHFS_OPT("all",        rename_workaround, 1),
	SSHFS_OPT("all",        nodelay_workaround, 1),
	SSHFS_OPT("all",        nodelaysrv_workaround, 1),
	SSHFS_OPT("all",        truncate_workaround, 1),
	SSHFS_OPT("all",        buflimit_workaround, 1),
	SSHFS_OPT("rename",     rename_workaround, 1),
	SSHFS_OPT("norename",   rename_workaround, 0),
	SSHFS_OPT("nodelay",    nodelay_workaround, 1),
	SSHFS_OPT("nonodelay",  nodelay_workaround, 0),
	SSHFS_OPT("nodelaysrv", nodelaysrv_workaround, 1),
	SSHFS_OPT("nonodelaysrv", nodelaysrv_workaround, 0),
	SSHFS_OPT("truncate",   truncate_workaround, 1),
	SSHFS_OPT("notruncate", truncate_workaround, 0),
	SSHFS_OPT("buflimit",   buflimit_workaround, 1),
	SSHFS_OPT("nobuflimit", buflimit_workaround, 0),
	FUSE_OPT_END
};

static struct fuse_cache_operations sshfs_oper;

static void error2(int errnum, const char *format, ...)
{
	va_list argv;
	fflush(stdout);
	fprintf(stderr, "error: ");
	va_start(argv, format);
	vfprintf(stderr, format, argv);
	va_end(argv);
	fprintf(stderr, ": %s\n", strerror(errnum));
}

static void perror2(const char *format, ...)
{
	va_list argv;
	fflush(stdout);
	fprintf(stderr, "error: ");
	va_start(argv, format);
	vfprintf(stderr, format, argv);
	va_end(argv);
	fprintf(stderr, ": %s\n", strerror(errno));
}

#define error3(format, ...) \
	fprintf(stderr, "error: "format"\n", ## __VA_ARGS__)
#define fatal(status, format, ...) \
	{perror2(format, ## __VA_ARGS__); exit(status);}
#define add_path(path) \
	g_strdup_printf("%s/%s", cynk.local, path[1] ? path + 1 : "")
#define is_online(path) \
	g_str_has_prefix(path, cynk.online_dir)


#define DEBUG(format, args...) \
	do { if (cynk.debug) fprintf(stderr, format, args); } while(0)

static const char *type_name(uint8_t type)
{
	switch(type) {
	case SSH_FXP_INIT:           return "INIT";
	case SSH_FXP_VERSION:        return "VERSION";
	case SSH_FXP_OPEN:           return "OPEN";
	case SSH_FXP_CLOSE:          return "CLOSE";
	case SSH_FXP_READ:           return "READ";
	case SSH_FXP_WRITE:          return "WRITE";
	case SSH_FXP_LSTAT:          return "LSTAT";
	case SSH_FXP_FSTAT:          return "FSTAT";
	case SSH_FXP_SETSTAT:        return "SETSTAT";
	case SSH_FXP_FSETSTAT:       return "FSETSTAT";
	case SSH_FXP_OPENDIR:        return "OPENDIR";
	case SSH_FXP_READDIR:        return "READDIR";
	case SSH_FXP_REMOVE:         return "REMOVE";
	case SSH_FXP_MKDIR:          return "MKDIR";
	case SSH_FXP_RMDIR:          return "RMDIR";
	case SSH_FXP_REALPATH:       return "REALPATH";
	case SSH_FXP_STAT:           return "STAT";
	case SSH_FXP_RENAME:         return "RENAME";
	case SSH_FXP_READLINK:       return "READLINK";
	case SSH_FXP_SYMLINK:        return "SYMLINK";
	case SSH_FXP_STATUS:         return "STATUS";
	case SSH_FXP_HANDLE:         return "HANDLE";
	case SSH_FXP_DATA:           return "DATA";
	case SSH_FXP_NAME:           return "NAME";
	case SSH_FXP_ATTRS:          return "ATTRS";
	case SSH_FXP_EXTENDED:       return "EXTENDED";
	case SSH_FXP_EXTENDED_REPLY: return "EXTENDED_REPLY";
	default:                     return "???";
	}
}

#define container_of(ptr, type, member) ({				\
			const typeof( ((type *)0)->member ) *__mptr = (ptr); \
			(type *)( (char *)__mptr - offsetof(type,member) );})

#define list_entry(ptr, type, member)		\
	container_of(ptr, type, member)

static int is_ssh_opt(const char *);
static void ssh_add_arg(const char *);

static void list_init(struct list_head *head)
{
	head->next = head;
	head->prev = head;
}

static void list_add(struct list_head *new, struct list_head *head)
{
	struct list_head *prev = head;
	struct list_head *next = head->next;
	next->prev = new;
	new->next = next;
	new->prev = prev;
	prev->next = new;
}

static void list_del(struct list_head *entry)
{
	struct list_head *prev = entry->prev;
	struct list_head *next = entry->next;
	next->prev = prev;
	prev->next = next;

}


static int list_empty(const struct list_head *head)
{
	return head->next == head;
}

static inline void buf_init(struct buffer *buf, size_t size)
{
	if (size) {
		buf->p = (uint8_t *) malloc(size);
		if (!buf->p) {
			fprintf(stderr, "sshfs: memory allocation failed\n");
			abort();
		}
	} else
		buf->p = NULL;
	buf->len = 0;
	buf->size = size;
}

static inline void buf_free(struct buffer *buf)
{
	free(buf->p);
}

static inline void buf_finish(struct buffer *buf)
{
	buf->len = buf->size;
}

static inline void buf_clear(struct buffer *buf)
{
	buf_free(buf);
	buf_init(buf, 0);
}

static void buf_resize(struct buffer *buf, size_t len)
{
	buf->size = (buf->len + len + 63) & ~31;
	buf->p = (uint8_t *) realloc(buf->p, buf->size);
	if (!buf->p) {
		fprintf(stderr, "sshfs: memory allocation failed\n");
		abort();
	}
}

static inline void buf_check_add(struct buffer *buf, size_t len)
{
	if (buf->len + len > buf->size)
		buf_resize(buf, len);
}

#define _buf_add_mem(b, d, l)			\
	buf_check_add(b, l);			\
	memcpy(b->p + b->len, d, l);		\
	b->len += l;


static inline void buf_add_mem(struct buffer *buf, const void *data,
                               size_t len)
{
	_buf_add_mem(buf, data, len);
}

static inline void buf_add_buf(struct buffer *buf, const struct buffer *bufa)
{
	_buf_add_mem(buf, bufa->p, bufa->len);
}

static inline void buf_add_uint8(struct buffer *buf, uint8_t val)
{
	_buf_add_mem(buf, &val, 1);
}

static inline void buf_add_uint32(struct buffer *buf, uint32_t val)
{
	uint32_t nval = htonl(val);
	_buf_add_mem(buf, &nval, 4);
}

static inline void buf_add_uint64(struct buffer *buf, uint64_t val)
{
	buf_add_uint32(buf, val >> 32);
	buf_add_uint32(buf, val & 0xffffffff);
}

static inline void buf_add_data(struct buffer *buf, const struct buffer *data)
{
	buf_add_uint32(buf, data->len);
	buf_add_mem(buf, data->p, data->len);
}

static inline void buf_add_string(struct buffer *buf, const char *str)
{
	struct buffer data;
	data.p = (uint8_t *) str;
	data.len = strlen(str);
	buf_add_data(buf, &data);
}

static inline void buf_add_path(struct buffer *buf, const char *path)
{
	char *realpath;

	realpath = g_strdup_printf("%s%s", cynk.base_path,
				   path[1] ? path+1 : ".");
	buf_add_string(buf, realpath);
	g_free(realpath);
}


static int buf_check_get(struct buffer *buf, size_t len)
{
	if (buf->len + len > buf->size) {
		fprintf(stderr, "buffer too short\n");
		return -1;
	} else
		return 0;
}

static inline int buf_get_mem(struct buffer *buf, void *data, size_t len)
{
	if (buf_check_get(buf, len) == -1)
		return -1;
	memcpy(data, buf->p + buf->len, len);
	buf->len += len;
	return 0;
}

static inline int buf_get_uint8(struct buffer *buf, uint8_t *val)
{
	return buf_get_mem(buf, val, 1);
}

static inline int buf_get_uint32(struct buffer *buf, uint32_t *val)
{
	uint32_t nval;
	if (buf_get_mem(buf, &nval, 4) == -1)
		return -1;
	*val = ntohl(nval);
	return 0;
}

static inline int buf_get_uint64(struct buffer *buf, uint64_t *val)
{
	uint32_t val1;
	uint32_t val2;
	if (buf_get_uint32(buf, &val1) == -1 ||
	    buf_get_uint32(buf, &val2) == -1) {
		return -1;
	}
	*val = ((uint64_t) val1 << 32) + val2;
	return 0;
}

static inline int buf_get_data(struct buffer *buf, struct buffer *data)
{
	uint32_t len;
	if (buf_get_uint32(buf, &len) == -1 || len > buf->size - buf->len)
		return -1;
	buf_init(data, len + 1);
	data->size = len;
	if (buf_get_mem(buf, data->p, data->size) == -1) {
		buf_free(data);
		return -1;
	}
	return 0;
}

static inline int buf_get_string(struct buffer *buf, char **str)
{
	struct buffer data;
	if (buf_get_data(buf, &data) == -1)
		return -1;
	data.p[data.size] = '\0';
	*str = (char *) data.p;
	return 0;
}

static int buf_get_attrs(struct buffer *buf, struct stat *stbuf, int *flagsp)
{
	uint32_t flags;
	uint64_t size = 0;
	uint32_t uid = 0;
	uint32_t gid = 0;
	uint32_t atime = 0;
	uint32_t mtime = 0;
	uint32_t mode = S_IFREG | 0777;

	if (buf_get_uint32(buf, &flags) == -1)
		return -1;
	if (flagsp)
		*flagsp = flags;
	if ((flags & SSH_FILEXFER_ATTR_SIZE) &&
	    buf_get_uint64(buf, &size) == -1)
		return -1;
	if ((flags & SSH_FILEXFER_ATTR_UIDGID) &&
	    (buf_get_uint32(buf, &uid) == -1 ||
	     buf_get_uint32(buf, &gid) == -1))
		return -1;
	if ((flags & SSH_FILEXFER_ATTR_PERMISSIONS) &&
	    buf_get_uint32(buf, &mode) == -1)
		return -1;
	if ((flags & SSH_FILEXFER_ATTR_ACMODTIME)) {
		if (buf_get_uint32(buf, &atime) == -1 ||
		    buf_get_uint32(buf, &mtime) == -1)
			return -1;
	}
	if ((flags & SSH_FILEXFER_ATTR_EXTENDED)) {
		uint32_t extcount;
		unsigned i;
		if (buf_get_uint32(buf, &extcount) == -1)
			return -1;
		for (i = 0; i < extcount; i++) {
			struct buffer tmp;
			if (buf_get_data(buf, &tmp) == -1)
				return -1;
			buf_free(&tmp);
			if (buf_get_data(buf, &tmp) == -1)
				return -1;
			buf_free(&tmp);
		}
	}

	if (cynk.remote_uid_detected && uid == cynk.remote_uid)
		uid = cynk.local_uid;

	memset(stbuf, 0, sizeof(struct stat));
	stbuf->st_mode = mode;
	stbuf->st_nlink = 1;
	stbuf->st_size = size;
	if (cynk.blksize) {
		stbuf->st_blksize = cynk.blksize;
		stbuf->st_blocks = ((size + cynk.blksize - 1) &
			~((unsigned long long) cynk.blksize - 1)) >> 9;
	}
	stbuf->st_uid = uid;
	stbuf->st_gid = gid;
	stbuf->st_atime = atime;
	stbuf->st_ctime = stbuf->st_mtime = mtime;
	return 0;
}

static int buf_get_statvfs(struct buffer *buf, struct statvfs *stbuf)
{
	uint64_t bsize;
	uint64_t frsize;
	uint64_t blocks;
	uint64_t bfree;
	uint64_t bavail;
	uint64_t files;
	uint64_t ffree;
	uint64_t favail;
	uint64_t fsid;
	uint64_t flag;
	uint64_t namemax;

	if (buf_get_uint64(buf, &bsize) == -1 ||
	    buf_get_uint64(buf, &frsize) == -1 ||
	    buf_get_uint64(buf, &blocks) == -1 ||
	    buf_get_uint64(buf, &bfree) == -1 ||
	    buf_get_uint64(buf, &bavail) == -1 ||
	    buf_get_uint64(buf, &files) == -1 ||
	    buf_get_uint64(buf, &ffree) == -1 ||
	    buf_get_uint64(buf, &favail) == -1 ||
	    buf_get_uint64(buf, &fsid) == -1 ||
	    buf_get_uint64(buf, &flag) == -1 ||
	    buf_get_uint64(buf, &namemax) == -1) {
		return -1;
	}

	memset(stbuf, 0, sizeof(struct statvfs));
	stbuf->f_bsize = bsize;
	stbuf->f_frsize = frsize;
	stbuf->f_blocks = blocks;
	stbuf->f_bfree = bfree;
	stbuf->f_bavail = bavail;
	stbuf->f_files = files;
	stbuf->f_ffree = ffree;
	stbuf->f_favail = favail;
	stbuf->f_namemax = namemax;

	return 0;
}

static int buf_get_entries(const char *path, struct buffer *buf, 
	void* fill_buf, fuse_fill_dir_t filler, struct cynk_dir *d)
{
	uint32_t count;
	unsigned i;

	if (buf_get_uint32(buf, &count) == -1)
		return -1;

	for (i = 0; i < count; i++) {
		int err = -1;
		char *name;
		char *longname;
		struct stat stbuf;
		if (buf_get_string(buf, &name) == -1)
			return -1;
		if (buf_get_string(buf, &longname) != -1) {
			free(longname);
			if (buf_get_attrs(buf, &stbuf, NULL) != -1) {
				if (cynk.follow_symlinks && S_ISLNK(stbuf.st_mode)) {
					stbuf.st_mode = 0;
				}
				err = filler(fill_buf, name, &stbuf, 0);
				if (!err) {
					g_ptr_array_add(d->dir, g_strdup(name));
					if (stbuf.st_mode & S_IFMT) {
						char *fullpath;
						const char *basepath = !path[1] ? "" : path;
						fullpath = g_strdup_printf("%s/%s", basepath, name);
						cache_add_attr(fullpath, &stbuf, d->wrctr);
						g_free(fullpath);
					}
				}
				err = 0;
			}
		}
		free(name);
		if (err)
			return err;
	}
	return 0;
}


static void ssh_add_arg(const char *arg)
{
	if (fuse_opt_add_arg(&cynk.ssh_args, arg) == -1)
		_exit(1);
}

#ifdef SSH_NODELAY_WORKAROUND
static int do_ssh_nodelay_workaround(void)
{
	char *oldpreload = getenv("LD_PRELOAD");
	char *newpreload;
	char sopath[PATH_MAX];
	int res;

	snprintf(sopath, sizeof(sopath), "%s/%s", LIBDIR, SSHNODELAY_SO);
	res = access(sopath, R_OK);
	if (res == -1) {
		char *s;
		if (!realpath(cynk.progname, sopath))
			return -1;

		s = strrchr(sopath, '/');
		if (!s)
			s = sopath;
		else
			s++;

		if (s + strlen(SSHNODELAY_SO) >= sopath + sizeof(sopath))
			return -1;

		strcpy(s, SSHNODELAY_SO);
		res = access(sopath, R_OK);
		if (res == -1) {
			fprintf(stderr, "sshfs: cannot find %s\n",
				SSHNODELAY_SO);
			return -1;
		}
	}

	newpreload = g_strdup_printf("%s%s%s",
				     oldpreload ? oldpreload : "",
				     oldpreload ? " " : "",
				     sopath);

	if (!newpreload || setenv("LD_PRELOAD", newpreload, 1) == -1) {
		fprintf(stderr, "warning: failed set LD_PRELOAD "
			"for ssh nodelay workaround\n");
	}
	g_free(newpreload);
	return 0;
}
#endif

static int pty_expect_loop(void)
{
	int res;
	char buf[256];
	const char *passwd_str = "assword:";
	int timeout = 60 * 1000; /* 1min timeout for the prompt to appear */
	int passwd_len = strlen(passwd_str);
	int len = 0;
	char c;

	while (1) {
		struct pollfd fds[2];

		fds[0].fd = cynk.fd;
		fds[0].events = POLLIN;
		fds[1].fd = cynk.ptyfd;
		fds[1].events = POLLIN;
		res = poll(fds, 2, timeout);
		if (res == -1) {
			perror("poll");
			return -1;
		}
		if (res == 0) {
			fprintf(stderr, "Timeout waiting for prompt\n");
			return -1;
		}
		if (fds[0].revents) {
			/*
			 * Something happened on stdout of ssh, this
			 * either means, that we are connected, or
			 * that we are disconnected.  In any case the
			 * password doesn't matter any more.
			 */
			break;
		}

		res = read(cynk.ptyfd, &c, 1);
		if (res == -1) {
			perror("read");
			return -1;
		}
		if (res == 0) {
			fprintf(stderr, "EOF while waiting for prompt\n");
			return -1;
		}
		buf[len] = c;
		len++;
		if (len == passwd_len) {
			if (memcmp(buf, passwd_str, passwd_len) == 0) {
				res = write(cynk.ptyfd, cynk.password,
				      strlen(cynk.password));
			}
			memmove(buf, buf + 1, passwd_len - 1);
			len--;
		}
	}

	if (!cynk.reconnect) {
		size_t size = getpagesize();

		memset(cynk.password, 0, size);
		munmap(cynk.password, size);
		cynk.password = NULL;
	}

	return 0;
}

static int pty_master(char **name)
{
	int mfd;

	mfd = open("/dev/ptmx", O_RDWR | O_NOCTTY);
	if (mfd == -1) {
		perror("failed to open pty");
		return -1;
	}
	if (grantpt(mfd) != 0) {
		perror("grantpt");
		return -1;
	}
	if (unlockpt(mfd) != 0) {
		perror("unlockpt");
		return -1;
	}
	*name = ptsname(mfd);

	return mfd;
}

static void replace_arg(char **argp, const char *newarg)
{
	free(*argp);
	*argp = strdup(newarg);
	if (*argp == NULL) {
		fprintf(stderr, "sshfs: memory allocation failed\n");
		abort();
	}
}

static int start_ssh(void)
{
	char *ptyname = NULL;
	int sockpair[2];
	int pid;
	int res;

	if (cynk.password_stdin) {

		cynk.ptyfd = pty_master(&ptyname);
		if (cynk.ptyfd == -1)
			return -1;

		cynk.ptyslavefd = open(ptyname, O_RDWR | O_NOCTTY);
		if (cynk.ptyslavefd == -1)
			return -1;
	}

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockpair) == -1) {
		perror("failed to create socket pair");
		return -1;
	}
	cynk.fd = sockpair[0];

	pid = fork();
	if (pid == -1) {
		perror("failed to fork");
		close(sockpair[1]);
		return -1;
	} else if (pid == 0) {
		int devnull;

#ifdef SSH_NODELAY_WORKAROUND
		if (cynk.nodelay_workaround &&
		    do_ssh_nodelay_workaround() == -1) {
			fprintf(stderr, "warning: ssh nodelay workaround disabled\n");
		}
#endif

		if (cynk.nodelaysrv_workaround) {
			int i;
			/*
			 * Hack to work around missing TCP_NODELAY
			 * setting in sshd
			 */
			for (i = 1; i < cynk.ssh_args.argc; i++) {
				if (strcmp(cynk.ssh_args.argv[i], "-x") == 0) {
					replace_arg(&cynk.ssh_args.argv[i], "-X");
					break;
				}
			}
		}

		devnull = open("/dev/null", O_WRONLY);

		if (dup2(sockpair[1], 0) == -1 || dup2(sockpair[1], 1) == -1) {
			perror("failed to redirect input/output");
			_exit(1);
		}
		if (!cynk.foreground && devnull != -1)
			dup2(devnull, 2);

		close(devnull);
		close(sockpair[0]);
		close(sockpair[1]);

		switch (fork()) {
		case -1:
			perror("failed to fork");
			_exit(1);
		case 0:
			break;
		default:
			_exit(0);
		}
		res = chdir("/");

		if (cynk.password_stdin) {
			int sfd;

			setsid();
			sfd = open(ptyname, O_RDWR);
			if (sfd == -1) {
				perror(ptyname);
				_exit(1);
			}
			close(sfd);
			close(cynk.ptyslavefd);
			close(cynk.ptyfd);
		}

		if (cynk.debug) {
			int i;

			fprintf(stderr, "executing");
			for (i = 0; i < cynk.ssh_args.argc; i++)
				fprintf(stderr, " <%s>",
					cynk.ssh_args.argv[i]);
			fprintf(stderr, "\n");
		}

		execvp(cynk.ssh_args.argv[0], cynk.ssh_args.argv);
		fprintf(stderr, "failed to execute '%s': %s\n",
			cynk.ssh_args.argv[0], strerror(errno));
		_exit(1);
	}
	waitpid(pid, NULL, 0);
	close(sockpair[1]);
	return 0;
}

static int connect_to(char *host, char *port)
{
	int err;
	int sock;
	int opt;
	struct addrinfo *ai;
	struct addrinfo hint;

	memset(&hint, 0, sizeof(hint));
	hint.ai_family = PF_INET;
	hint.ai_socktype = SOCK_STREAM;
	err = getaddrinfo(host, port, &hint, &ai);
	if (err) {
		fprintf(stderr, "failed to resolve %s:%s: %s\n", host, port,
			gai_strerror(err));
		return -1;
	}
	sock = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
	if (sock == -1) {
		perror("failed to create socket");
		return -1;
	}
	err = connect(sock, ai->ai_addr, ai->ai_addrlen);
	if (err == -1) {
		perror("failed to connect");
		return -1;
	}
	opt = 1;
	err = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
	if (err == -1)
		perror("warning: failed to set TCP_NODELAY");

	freeaddrinfo(ai);

	cynk.fd = sock;
	return 0;
}

static int do_write(struct iovec *iov, size_t count)
{
	int res;
	while (count) {
		res = writev(cynk.fd, iov, count);
		if (res == -1) {
			perror("write");
			return -1;
		} else if (res == 0) {
			fprintf(stderr, "zero write\n");
			return -1;
		}
		do {
			if ((unsigned) res < iov->iov_len) {
				iov->iov_len -= res;
				iov->iov_base += res;
				break;
			} else {
				res -= iov->iov_len;
				count --;
				iov ++;
			}
		} while(count);
	}
	return 0;
}

static uint32_t sftp_get_id(void)
{
	static uint32_t idctr;
	return idctr++;
}


static void buf_to_iov(const struct buffer *buf, struct iovec *iov)
{
	iov->iov_base = buf->p;
	iov->iov_len = buf->len;
}

static size_t iov_length(const struct iovec *iov, unsigned long nr_segs)
{
	unsigned long seg;
	size_t ret = 0;

	for (seg = 0; seg < nr_segs; seg++)
		ret += iov[seg].iov_len;
	return ret;
}

#define SFTP_MAX_IOV 3

static int sftp_send_iov(uint8_t type, uint32_t id, struct iovec iov[],
                         size_t count)
{
	int res;
	struct buffer buf;
	struct iovec iovout[SFTP_MAX_IOV];
	unsigned i;
	unsigned nout = 0;

	assert(count <= SFTP_MAX_IOV - 1);
	buf_init(&buf, 9);
	buf_add_uint32(&buf, iov_length(iov, count) + 5);
	buf_add_uint8(&buf, type);
	buf_add_uint32(&buf, id);
	buf_to_iov(&buf, &iovout[nout++]);
	for (i = 0; i < count; i++)
		iovout[nout++] = iov[i];
	pthread_mutex_lock(&cynk.lock_write);
	res = do_write(iovout, nout);
	pthread_mutex_unlock(&cynk.lock_write);
	buf_free(&buf);
	return res;
}

static int do_read(struct buffer *buf)
{
	int res;
	uint8_t *p = buf->p;
	size_t size = buf->size;
	while (size) {
		res = read(cynk.fd, p, size);
		if (res == -1) {
			perror("read");
			return -1;
		} else if (res == 0) {
			fprintf(stderr, "remote host has disconnected\n");
			return -1;
		}
		size -= res;
		p += res;
	}
	return 0;
}

static int sftp_read(uint8_t *type, struct buffer *buf)
{
	int res;
	struct buffer buf2;
	uint32_t len;
	buf_init(&buf2, 5);
	res = do_read(&buf2);
	if (res != -1) {
		if (buf_get_uint32(&buf2, &len) == -1)
			return -1;
		if (len > MAX_REPLY_LEN) {
			fprintf(stderr, "reply len too large: %u\n", len);
			return -1;
		}
		if (buf_get_uint8(&buf2, type) == -1)
			return -1;
		buf_init(buf, len - 1);
		res = do_read(buf);
	}
	buf_free(&buf2);
	return res;
}

static void request_free(struct request *req)
{
	buf_free(&req->reply);
	sem_destroy(&req->ready);
	g_free(req);
}

static void chunk_free(struct read_chunk *chunk)
{
	buf_free(&chunk->data);
	sem_destroy(&chunk->ready);
	g_free(chunk);
}

static void chunk_put(struct read_chunk *chunk)
{
	if (chunk) {
		chunk->refs--;
		if (!chunk->refs)
			chunk_free(chunk);
	}
}


static void chunk_put_locked(struct read_chunk *chunk)
{
	pthread_mutex_lock(&cynk.lock);
	chunk_put(chunk);
	pthread_mutex_unlock(&cynk.lock);
}

static int clean_req(void *key_, struct request *req)
{
	(void) key_;

	req->error = -EIO;
	if (req->want_reply)
		sem_post(&req->ready);
	else {
		if (req->end_func)
			req->end_func(req);
		request_free(req);
	}
	return TRUE;
}

static int process_one_request(void)
{
	int res;
	struct buffer buf;
	uint8_t type;
	struct request *req;
	uint32_t id;

	buf_init(&buf, 0);
	res = sftp_read(&type, &buf);
	if (res == -1)
		return -1;
	if (buf_get_uint32(&buf, &id) == -1)
		return -1;

	pthread_mutex_lock(&cynk.lock);
	req = (struct request *)
		g_hash_table_lookup(cynk.reqtab, GUINT_TO_POINTER(id));
	if (req == NULL)
		fprintf(stderr, "request %i not found\n", id);
	else {
		int was_over;

		was_over = cynk.outstanding_len > cynk.max_outstanding_len;
		cynk.outstanding_len -= req->len;
		if (was_over &&
		    cynk.outstanding_len <= cynk.max_outstanding_len) {
			pthread_cond_broadcast(&cynk.outstanding_cond);
		}
		g_hash_table_remove(cynk.reqtab, GUINT_TO_POINTER(id));
	}
	pthread_mutex_unlock(&cynk.lock);
	if (req != NULL) {
		if (cynk.debug) {
			struct timeval now;
			unsigned int difftime;
			unsigned msgsize = buf.size + 5;

			gettimeofday(&now, NULL);
			difftime = (now.tv_sec - req->start.tv_sec) * 1000;
			difftime += (now.tv_usec - req->start.tv_usec) / 1000;
			DEBUG("  [%05i] %14s %8ubytes (%ims)\n", id,
			      type_name(type), msgsize, difftime);

			if (difftime < cynk.min_rtt || !cynk.num_received)
				cynk.min_rtt = difftime;
			if (difftime > cynk.max_rtt)
				cynk.max_rtt = difftime;
			cynk.total_rtt += difftime;
			cynk.num_received++;
			cynk.bytes_received += msgsize;
		}
		req->reply = buf;
		req->reply_type = type;
		req->replied = 1;
		if (req->want_reply)
			sem_post(&req->ready);
		else {
			if (req->end_func) {
				pthread_mutex_lock(&cynk.lock);
				req->end_func(req);
				pthread_mutex_unlock(&cynk.lock);
			}
			request_free(req);
		}
	} else
		buf_free(&buf);

	return 0;
}

static void close_conn(void)
{
	close(cynk.fd);
	cynk.fd = -1;
	if (cynk.ptyfd != -1) {
		close(cynk.ptyfd);
		cynk.ptyfd = -1;
	}
	if (cynk.ptyslavefd != -1) {
		close(cynk.ptyslavefd);
		cynk.ptyslavefd = -1;
	}
}

static void *process_requests(void *data_)
{
	(void) data_;

	while (1) {
		if (process_one_request() == -1)
			break;
	}

	if (!cynk.reconnect) {
		/* harakiri */
		kill(getpid(), SIGTERM);
	} else {
		pthread_mutex_lock(&cynk.lock);
		cynk.processing_thread_started = 0;
		close_conn();
		g_hash_table_foreach_remove(cynk.reqtab, (GHRFunc) clean_req,
					    NULL);
		cynk.connver ++;
		pthread_mutex_unlock(&cynk.lock);
	}
	return NULL;
}

static int sftp_init_reply_ok(struct buffer *buf, uint32_t *version)
{
	uint32_t len;
	uint8_t type;

	if (buf_get_uint32(buf, &len) == -1)
		return -1;

	if (len < 5 || len > MAX_REPLY_LEN)
		return 1;

	if (buf_get_uint8(buf, &type) == -1)
		return -1;

	if (type != SSH_FXP_VERSION)
		return 1;

	if (buf_get_uint32(buf, version) == -1)
		return -1;

	DEBUG("Server version: %u\n", *version);

	if (len > 5) {
		struct buffer buf2;

		buf_init(&buf2, len - 5);
		if (do_read(&buf2) == -1)
			return -1;

		do {
			char *ext;
			char *extdata;

			if (buf_get_string(&buf2, &ext) == -1 ||
			    buf_get_string(&buf2, &extdata) == -1)
				return -1;

			DEBUG("Extension: %s <%s>\n", ext, extdata);

			if (strcmp(ext, SFTP_EXT_POSIX_RENAME) == 0 &&
			    strcmp(extdata, "1") == 0) {
				cynk.ext_posix_rename = 1;
				cynk.rename_workaround = 0;
			}
			if (strcmp(ext, SFTP_EXT_STATVFS) == 0 &&
			    strcmp(extdata, "2") == 0)
				cynk.ext_statvfs = 1;
		} while (buf2.len < buf2.size);
	}
	return 0;
}

static int sftp_find_init_reply(uint32_t *version)
{
	int res;
	struct buffer buf;

	buf_init(&buf, 9);
	res = do_read(&buf);
	while (res != -1) {
		struct buffer buf2;

		res = sftp_init_reply_ok(&buf, version);
		if (res <= 0)
			break;

		/* Iterate over any rubbish until the version reply is found */
		DEBUG("%c", *buf.p);
		memmove(buf.p, buf.p + 1, buf.size - 1);
		buf.len = 0;
		buf2.p = buf.p + buf.size - 1;
		buf2.size = 1;
		res = do_read(&buf2);
	}
	buf_free(&buf);
	return res;
}

static int sftp_init()
{
	int res = -1;
	uint32_t version = 0;
	struct buffer buf;
	buf_init(&buf, 0);
	if (sftp_send_iov(SSH_FXP_INIT, PROTO_VERSION, NULL, 0) == -1)
		goto out;

	if (cynk.password_stdin && pty_expect_loop() == -1)
		goto out;

	if (sftp_find_init_reply(&version) == -1)
		goto out;

	cynk.server_version = version;
	if (version > PROTO_VERSION) {
		fprintf(stderr,
			"Warning: server uses version: %i, we support: %i\n",
			version, PROTO_VERSION);
	}
	res = 0;

out:
	buf_free(&buf);
	return res;
}

static int sftp_error_to_errno(uint32_t error)
{
	switch (error) {
	case SSH_FX_OK:                return 0;
	case SSH_FX_NO_SUCH_FILE:      return ENOENT;
	case SSH_FX_PERMISSION_DENIED: return EACCES;
	case SSH_FX_FAILURE:           return EPERM;
	case SSH_FX_BAD_MESSAGE:       return EBADMSG;
	case SSH_FX_NO_CONNECTION:     return ENOTCONN;
	case SSH_FX_CONNECTION_LOST:   return ECONNABORTED;
	case SSH_FX_OP_UNSUPPORTED:    return EOPNOTSUPP;
	default:                       return EIO;
	}
}

static void sftp_detect_uid()
{
	int flags;
	uint32_t id = sftp_get_id();
	uint32_t replid;
	uint8_t type;
	struct buffer buf;
	struct stat stbuf;
	struct iovec iov[1];

	buf_init(&buf, 5);
	buf_add_string(&buf, ".");
	buf_to_iov(&buf, &iov[0]);
	if (sftp_send_iov(SSH_FXP_STAT, id, iov, 1) == -1)
		goto out;
	buf_clear(&buf);
	if (sftp_read(&type, &buf) == -1)
		goto out;
	if (type != SSH_FXP_ATTRS && type != SSH_FXP_STATUS) {
		fprintf(stderr, "protocol error\n");
		goto out;
	}
	if (buf_get_uint32(&buf, &replid) == -1)
		goto out;
	if (replid != id) {
		fprintf(stderr, "bad reply ID\n");
		goto out;
	}
	if (type == SSH_FXP_STATUS) {
		uint32_t serr;
		if (buf_get_uint32(&buf, &serr) == -1)
			goto out;

		fprintf(stderr, "failed to stat home directory (%i)\n", serr);
		goto out;
	}
	if (buf_get_attrs(&buf, &stbuf, &flags) == -1)
		goto out;

	if (!(flags & SSH_FILEXFER_ATTR_UIDGID))
		goto out;

	cynk.remote_uid = stbuf.st_uid;
	cynk.local_uid = getuid();
	cynk.remote_uid_detected = 1;
	DEBUG("remote_uid = %i\n", cynk.remote_uid);

out:
	if (!cynk.remote_uid_detected)
		fprintf(stderr, "failed to detect remote user ID\n");

	buf_free(&buf);
}

static int sftp_check_root(const char *base_path)
{
	int flags;
	uint32_t id = sftp_get_id();
	uint32_t replid;
	uint8_t type;
	struct buffer buf;
	struct stat stbuf;
	struct iovec iov[1];
	int err = -1;
	const char *remote_dir = base_path[0] ? base_path : ".";

	buf_init(&buf, 0);
	buf_add_string(&buf, remote_dir);
	buf_to_iov(&buf, &iov[0]);
	if (sftp_send_iov(SSH_FXP_STAT, id, iov, 1) == -1)
		goto out;
	buf_clear(&buf);
	if (sftp_read(&type, &buf) == -1)
		goto out;
	if (type != SSH_FXP_ATTRS && type != SSH_FXP_STATUS) {
		fprintf(stderr, "protocol error\n");
		goto out;
	}
	if (buf_get_uint32(&buf, &replid) == -1)
		goto out;
	if (replid != id) {
		fprintf(stderr, "bad reply ID\n");
		goto out;
	}
	if (type == SSH_FXP_STATUS) {
		uint32_t serr;
		if (buf_get_uint32(&buf, &serr) == -1)
			goto out;

		fprintf(stderr, "%s:%s: %s\n", cynk.host, remote_dir,
			strerror(sftp_error_to_errno(serr)));

		goto out;
	}
	if (buf_get_attrs(&buf, &stbuf, &flags) == -1)
		goto out;

	if (!(flags & SSH_FILEXFER_ATTR_PERMISSIONS))
		goto out;

	if (!S_ISDIR(stbuf.st_mode)) {
		fprintf(stderr, "%s:%s: Not a directory\n", cynk.host,
			remote_dir);
		goto out;
	}

	err = 0;

out:
	buf_free(&buf);
	return err;
}


static int connect_remote(void)
{
	int err;

	if (cynk.directport)
		err = connect_to(cynk.host, cynk.directport);
	else
		err = start_ssh();
	if (!err)
		err = sftp_init();

	if (err)
		close_conn();
	else
		cynk.num_connect++;

	return err;
}

static int start_processing_thread(void)
{
	int err;
	pthread_t thread_id;
	sigset_t oldset;
	sigset_t newset;

	if (cynk.processing_thread_started)
		return 0;

	if (cynk.fd == -1) {
		err = connect_remote();
		if (err)
			return -EIO;
	}

	sigemptyset(&newset);
	sigaddset(&newset, SIGTERM);
	sigaddset(&newset, SIGINT);
	sigaddset(&newset, SIGHUP);
	sigaddset(&newset, SIGQUIT);
	pthread_sigmask(SIG_BLOCK, &newset, &oldset);
	err = pthread_create(&thread_id, NULL, process_requests, NULL);
	if (err) {
		fprintf(stderr, "failed to create thread: %s\n", strerror(err));
		return -EIO;
	}
	pthread_detach(thread_id);
	pthread_sigmask(SIG_SETMASK, &oldset, NULL);
	cynk.processing_thread_started = 1;
	return 0;
}

#ifdef SSHFS_USE_INIT
#if FUSE_VERSION >= 26
static void *sshfs_init(struct fuse_conn_info *conn)
#else
static void *sshfs_init(void)
#endif
{
#if FUSE_VERSION >= 26
	/* Readahead should be done by kernel or sshfs but not both */
	if (conn->async_read)
		cynk.sync_read = 1;
#endif

	if (cynk.detect_uid)
		sftp_detect_uid();

	start_processing_thread();
	return NULL;
}
#endif

static int sftp_request_wait(struct request *req, uint8_t type,
                             uint8_t expect_type, struct buffer *outbuf)
{
	int err;

	if (req->error) {
		err = req->error;
		goto out;
	}
	while (sem_wait(&req->ready));
	if (req->error) {
		err = req->error;
		goto out;
	}
	err = -EIO;
	if (req->reply_type != expect_type &&
	    req->reply_type != SSH_FXP_STATUS) {
		fprintf(stderr, "protocol error\n");
		goto out;
	}
	if (req->reply_type == SSH_FXP_STATUS) {
		uint32_t serr;
		if (buf_get_uint32(&req->reply, &serr) == -1)
			goto out;

		switch (serr) {
		case SSH_FX_OK:
			if (expect_type == SSH_FXP_STATUS)
				err = 0;
			else
				err = -EIO;
			break;

		case SSH_FX_EOF:
			if (type == SSH_FXP_READ || type == SSH_FXP_READDIR)
				err = MY_EOF;
			else
				err = -EIO;
			break;

		default:
			err = -sftp_error_to_errno(serr);
		}
	} else {
		buf_init(outbuf, req->reply.size - req->reply.len);
		buf_get_mem(&req->reply, outbuf->p, outbuf->size);
		err = 0;
	}

out:
	if (req->end_func) {
		pthread_mutex_lock(&cynk.lock);
		req->end_func(req);
		pthread_mutex_unlock(&cynk.lock);
	}
	request_free(req);
	return err;
}

static int sftp_request_send(uint8_t type, struct iovec *iov, size_t count,
                             request_func begin_func, request_func end_func,
                             int want_reply, void *data,
                             struct request **reqp)
{
	int err;
	uint32_t id;
	struct request *req = g_new0(struct request, 1);

	req->want_reply = want_reply;
	req->end_func = end_func;
	req->data = data;
	sem_init(&req->ready, 0, 0);
	buf_init(&req->reply, 0);
	pthread_mutex_lock(&cynk.lock);
	if (begin_func)
		begin_func(req);
	id = sftp_get_id();
	err = start_processing_thread();
	if (err) {
		pthread_mutex_unlock(&cynk.lock);
		goto out;
	}
	req->len = iov_length(iov, count) + 9;
	cynk.outstanding_len += req->len;
	while (cynk.outstanding_len > cynk.max_outstanding_len)
		pthread_cond_wait(&cynk.outstanding_cond, &cynk.lock);

	g_hash_table_insert(cynk.reqtab, GUINT_TO_POINTER(id), req);
	if (cynk.debug) {
		gettimeofday(&req->start, NULL);
		cynk.num_sent++;
		cynk.bytes_sent += req->len;
	}
	DEBUG("[%05i] %s\n", id, type_name(type));
	pthread_mutex_unlock(&cynk.lock);

	err = -EIO;
	if (sftp_send_iov(type, id, iov, count) == -1) {
		pthread_mutex_lock(&cynk.lock);
		g_hash_table_remove(cynk.reqtab, GUINT_TO_POINTER(id));
		pthread_mutex_unlock(&cynk.lock);
		goto out;
	}
	if (want_reply)
		*reqp = req;
	return 0;

out:
	req->error = err;
	if (!want_reply)
		sftp_request_wait(req, type, 0, NULL);
	else
		*reqp = req;

	return err;
}


static int sftp_request_iov(uint8_t type, struct iovec *iov, size_t count,
                            uint8_t expect_type, struct buffer *outbuf)
{
	struct request *req;

	sftp_request_send(type, iov, count, NULL, NULL, expect_type, NULL,
			  &req);
	if (expect_type == 0)
		return 0;

	return sftp_request_wait(req, type, expect_type, outbuf);
}

static int sftp_request(uint8_t type, const struct buffer *buf,
                        uint8_t expect_type, struct buffer *outbuf)
{
	struct iovec iov;

	buf_to_iov(buf, &iov);
	return sftp_request_iov(type, &iov, 1, expect_type, outbuf);
}

static int sshfs_getattr(const char *path, struct stat *stbuf)
{
	int err;
	struct buffer buf;
	struct buffer outbuf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	err = sftp_request(cynk.follow_symlinks ? SSH_FXP_STAT : SSH_FXP_LSTAT,
			   &buf, SSH_FXP_ATTRS, &outbuf);
	if (!err) {
		if (buf_get_attrs(&outbuf, stbuf, NULL) == -1)
			err = -EIO;
		buf_free(&outbuf);
	}
	buf_free(&buf);
	return err;
}

static int count_components(const char *p)
{
	int ctr;

	for (; *p == '/'; p++);
	for (ctr = 0; *p; ctr++) {
		for (; *p && *p != '/'; p++);
		for (; *p == '/'; p++);
	}
	return ctr;
}

static void strip_common(const char **sp, const char **tp)
{
	const char *s = *sp;
	const char *t = *tp;
	do {
		for (; *s == '/'; s++);
		for (; *t == '/'; t++);
		*tp = t;
		*sp = s;
		for (; *s == *t && *s && *s != '/'; s++, t++);
	} while ((*s == *t && *s) || (!*s && *t == '/') || (*s == '/' && !*t));
}

static void transform_symlink(const char *path, char **linkp)
{
	const char *l = *linkp;
	const char *b = cynk.base_path;
	char *newlink;
	char *s;
	int dotdots;
	int i;

	if (l[0] != '/' || b[0] != '/')
		return;

	strip_common(&l, &b);
	if (*b)
		return;

	strip_common(&l, &path);
	dotdots = count_components(path);
	if (!dotdots)
		return;
	dotdots--;

	newlink = malloc(dotdots * 3 + strlen(l) + 2);
	if (!newlink) {
		fprintf(stderr, "sshfs: memory allocation failed\n");
		abort();
	}
	for (s = newlink, i = 0; i < dotdots; i++, s += 3)
		strcpy(s, "../");

	if (l[0])
		strcpy(s, l);
	else if (!dotdots)
		strcpy(s, ".");
	else
		s[0] = '\0';

	free(*linkp);
	*linkp = newlink;
}

static int sshfs_readlink(const char *path, char *linkbuf, size_t size)
{
	int err;
	struct buffer buf;
	struct buffer name;

	assert(size > 0);

	if (cynk.server_version < 3)
		return -EPERM;

	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	err = sftp_request(SSH_FXP_READLINK, &buf, SSH_FXP_NAME, &name);
	if (!err) {
		uint32_t count;
		char *link;
		err = -EIO;
		if(buf_get_uint32(&name, &count) != -1 && count == 1 &&
		   buf_get_string(&name, &link) != -1) {
			if (cynk.transform_symlinks)
				transform_symlink(path, &link);
			strncpy(linkbuf, link, size - 1);
			linkbuf[size - 1] = '\0';
			free(link);
			err = 0;
		}
		buf_free(&name);
	}
	buf_free(&buf);
	return err;
}



static int sshfs_mkdir(const char *path, mode_t mode)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_PERMISSIONS);
	buf_add_uint32(&buf, mode);
	err = sftp_request(SSH_FXP_MKDIR, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int sshfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int err;
	struct buffer buf;
	struct buffer handle;
	(void) rdev;

	if ((mode & S_IFMT) != S_IFREG)
		return -EPERM;

	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, SSH_FXF_WRITE | SSH_FXF_CREAT | SSH_FXF_EXCL);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_PERMISSIONS);
	buf_add_uint32(&buf, mode);
	err = sftp_request(SSH_FXP_OPEN, &buf, SSH_FXP_HANDLE, &handle);
	if (!err) {
		int err2;
		buf_finish(&handle);
		err2 = sftp_request(SSH_FXP_CLOSE, &handle, SSH_FXP_STATUS,
				    NULL);
		if (!err)
			err = err2;
		buf_free(&handle);
	}
	buf_free(&buf);
	return err;
}

static int sshfs_symlink(const char *from, const char *to)
{
	int err;
	struct buffer buf;

	if (cynk.server_version < 3)
		return -EPERM;

	/* openssh sftp server doesn't follow standard: link target and
	   link name are mixed up, so we must also be non-standard :( */
	buf_init(&buf, 0);
	buf_add_string(&buf, from);
	buf_add_path(&buf, to);
	err = sftp_request(SSH_FXP_SYMLINK, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int sshfs_unlink(const char *path)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	err = sftp_request(SSH_FXP_REMOVE, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int sshfs_rmdir(const char *path)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	err = sftp_request(SSH_FXP_RMDIR, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}


static int sshfs_do_rename(const char *from, const char *to)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_path(&buf, from);
	buf_add_path(&buf, to);
	err = sftp_request(SSH_FXP_RENAME, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int sshfs_ext_posix_rename(const char *from, const char *to)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_string(&buf, SFTP_EXT_POSIX_RENAME);
	buf_add_path(&buf, from);
	buf_add_path(&buf, to);
	err = sftp_request(SSH_FXP_EXTENDED, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static void random_string(char *str, int length)
{
	int i;
	for (i = 0; i < length; i++)
		*str++ = (char)('0' + rand_r(&cynk.randseed) % 10);
	*str = '\0';
}

static int sshfs_rename(const char *from, const char *to)
{
	int err;
	if (cynk.ext_posix_rename)
		err = sshfs_ext_posix_rename(from, to);
	else
		err = sshfs_do_rename(from, to);
	if (err == -EPERM && cynk.rename_workaround) {
		size_t tolen = strlen(to);
		if (tolen + RENAME_TEMP_CHARS < PATH_MAX) {
			int tmperr;
			char totmp[PATH_MAX];
			strcpy(totmp, to);
			random_string(totmp + tolen, RENAME_TEMP_CHARS);
			tmperr = sshfs_do_rename(to, totmp);
			if (!tmperr) {
				err = sshfs_do_rename(from, to);
				if (!err)
					err = sshfs_unlink(totmp);
				else
					sshfs_do_rename(totmp, to);
			}
		}
	}
	return err;
}

static int sshfs_chmod(const char *path, mode_t mode)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_PERMISSIONS);
	buf_add_uint32(&buf, mode);
	err = sftp_request(SSH_FXP_SETSTAT, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int sshfs_chown(const char *path, uid_t uid, gid_t gid)
{
	int err;
	struct buffer buf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_UIDGID);
	buf_add_uint32(&buf, uid);
	buf_add_uint32(&buf, gid);
	err = sftp_request(SSH_FXP_SETSTAT, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int sshfs_truncate_workaround(const char *path, off_t size,
                                     struct fuse_file_info *fi);

static int sshfs_truncate(const char *path, off_t size)
{
	int err;
	struct buffer buf;

	cynk.modifver ++;
	if (size == 0 || cynk.truncate_workaround)
		return sshfs_truncate_workaround(path, size, NULL);

	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_SIZE);
	buf_add_uint64(&buf, size);
	err = sftp_request(SSH_FXP_SETSTAT, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static inline int sshfs_file_is_conn(struct sshfs_file *sf)
{
	return sf->connver == cynk.connver;
}

static int sshfs_open_common(const char *path, mode_t mode,
                             struct fuse_file_info *fi)
{
	int err;
	int err2;
	struct buffer buf;
	struct buffer outbuf;
	struct stat stbuf;
	struct sshfs_file *sf;
	struct request *open_req;
	uint32_t pflags = 0;
	struct iovec iov;
	uint8_t type;
	uint64_t wrctr = cache_get_write_ctr();

	if ((fi->flags & O_ACCMODE) == O_RDONLY)
		pflags = SSH_FXF_READ;
	else if((fi->flags & O_ACCMODE) == O_WRONLY)
		pflags = SSH_FXF_WRITE;
	else if ((fi->flags & O_ACCMODE) == O_RDWR)
		pflags = SSH_FXF_READ | SSH_FXF_WRITE;
	else
		return -EINVAL;

	if (fi->flags & O_CREAT)
		pflags |= SSH_FXF_CREAT;

	if (fi->flags & O_EXCL)
		pflags |= SSH_FXF_EXCL;

	if (fi->flags & O_TRUNC)
		pflags |= SSH_FXF_TRUNC;

	sf = g_new0(struct sshfs_file, 1);
	list_init(&sf->write_reqs);
	pthread_cond_init(&sf->write_finished, NULL);
	/* Assume random read after open */
	sf->is_seq = 0;
	sf->refs = 1;
	sf->next_pos = 0;
	sf->modifver= cynk.modifver;
	sf->connver = cynk.connver;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, pflags);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_PERMISSIONS);
	buf_add_uint32(&buf, mode);
	buf_to_iov(&buf, &iov);
	sftp_request_send(SSH_FXP_OPEN, &iov, 1, NULL, NULL, 1, NULL,
			  &open_req);
	buf_clear(&buf);
	buf_add_path(&buf, path);
	type = cynk.follow_symlinks ? SSH_FXP_STAT : SSH_FXP_LSTAT;
	err2 = sftp_request(type, &buf, SSH_FXP_ATTRS, &outbuf);
	if (!err2) {
		if (buf_get_attrs(&outbuf, &stbuf, NULL) == -1)
			err2 = -EIO;
		buf_free(&outbuf);
	}
	err = sftp_request_wait(open_req, SSH_FXP_OPEN, SSH_FXP_HANDLE,
				&sf->handle);
	if (!err && err2) {
		buf_finish(&sf->handle);
		sftp_request(SSH_FXP_CLOSE, &sf->handle, 0, NULL);
		buf_free(&sf->handle);
		err = err2;
	}

	if (!err) {
		cache_add_attr(path, &stbuf, wrctr);
		buf_finish(&sf->handle);
		fi->fh = (unsigned long) sf;
	} else {
		cache_invalidate(path);
		g_free(sf);
	}
	buf_free(&buf);
	return err;
}

static int sshfs_open(const char *path, struct fuse_file_info *fi)
{
	return sshfs_open_common(path, 0, fi);
}

static inline struct sshfs_file *get_sshfs_file(struct fuse_file_info *fi)
{
	return (struct sshfs_file *) (uintptr_t) fi->fh;
}

static int sshfs_flush(const char *path, struct fuse_file_info *fi)
{
	int err;
	struct sshfs_file *sf = get_sshfs_file(fi);
	struct list_head write_reqs;
	struct list_head *curr_list;

	if (!sshfs_file_is_conn(sf))
		return -EIO;

	if (cynk.sync_write)
		return 0;

	(void) path;
	pthread_mutex_lock(&cynk.lock);
	if (!list_empty(&sf->write_reqs)) {
		curr_list = sf->write_reqs.prev;
		list_del(&sf->write_reqs);
		list_init(&sf->write_reqs);
		list_add(&write_reqs, curr_list);
		while (!list_empty(&write_reqs))
			pthread_cond_wait(&sf->write_finished, &cynk.lock);
	}
	err = sf->write_error;
	sf->write_error = 0;
	pthread_mutex_unlock(&cynk.lock);
	return err;
}


static int sshfs_fsync(const char *path, int isdatasync,
                       struct fuse_file_info *fi)
{
	(void) isdatasync;
	return sshfs_flush(path, fi);
}

static void sshfs_file_put(struct sshfs_file *sf)
{
	sf->refs--;
	if (!sf->refs)
		g_free(sf);
}

static void sshfs_file_get(struct sshfs_file *sf)
{
	sf->refs++;
}

static int sshfs_release(const char *path, struct fuse_file_info *fi)
{
	struct sshfs_file *sf = get_sshfs_file(fi);
	struct buffer *handle = &sf->handle;
	if (sshfs_file_is_conn(sf)) {
		sshfs_flush(path, fi);
		sftp_request(SSH_FXP_CLOSE, handle, 0, NULL);
	}
	buf_free(handle);
	chunk_put_locked(sf->readahead);
	sshfs_file_put(sf);
	return 0;
}

static int sshfs_sync_read(struct sshfs_file *sf, char *rbuf, size_t size,
                           off_t offset)
{
	int err;
	struct buffer buf;
	struct buffer data;
	struct buffer *handle = &sf->handle;
	buf_init(&buf, 0);
	buf_add_buf(&buf, handle);
	buf_add_uint64(&buf, offset);
	buf_add_uint32(&buf, size);
	err = sftp_request(SSH_FXP_READ, &buf, SSH_FXP_DATA, &data);
	if (!err) {
		uint32_t retsize;
		err = -EIO;
		if (buf_get_uint32(&data, &retsize) != -1) {
			if (retsize > size)
				fprintf(stderr, "long read\n");
			else {
				buf_get_mem(&data, rbuf, retsize);
				err = retsize;
			}
		}
		buf_free(&data);
	} else if (err == MY_EOF)
		err = 0;
	buf_free(&buf);
	return err;
}

static void sshfs_read_end(struct request *req)
{
	struct read_chunk *chunk = (struct read_chunk *) req->data;
	if (req->error)
		chunk->res = req->error;
	else if (req->replied) {
		chunk->res = -EIO;

		if (req->reply_type == SSH_FXP_STATUS) {
			uint32_t serr;
			if (buf_get_uint32(&req->reply, &serr) != -1) {
				if (serr == SSH_FX_EOF)
					chunk->res = 0;
			}
		} else if (req->reply_type == SSH_FXP_DATA) {
			uint32_t retsize;
			if (buf_get_uint32(&req->reply, &retsize) != -1) {
				if (retsize > chunk->size)
					fprintf(stderr, "long read\n");
				else {
					chunk->res = retsize;
					chunk->data = req->reply;
					buf_init(&req->reply, 0);
				}
			}
		} else
			fprintf(stderr, "protocol error\n");
	} else
		chunk->res = -EIO;

	sem_post(&chunk->ready);
	chunk_put(chunk);
}

static void sshfs_read_begin(struct request *req)
{
	struct read_chunk *chunk = (struct read_chunk *) req->data;
	chunk->refs++;
}

static void sshfs_send_async_read(struct sshfs_file *sf,
                                  struct read_chunk *chunk)
{
	struct buffer buf;
	struct buffer *handle = &sf->handle;
	struct iovec iov;

	buf_init(&buf, 0);
	buf_add_buf(&buf, handle);
	buf_add_uint64(&buf, chunk->offset);
	buf_add_uint32(&buf, chunk->size);
	buf_to_iov(&buf, &iov);
	sftp_request_send(SSH_FXP_READ, &iov, 1, sshfs_read_begin,
			  sshfs_read_end, 0, chunk, NULL);
	buf_free(&buf);
}

static void submit_read(struct sshfs_file *sf, size_t size, off_t offset,
                        struct read_chunk **chunkp)
{
	struct read_chunk *chunk = g_new0(struct read_chunk, 1);

	sem_init(&chunk->ready, 0, 0);
	buf_init(&chunk->data, 0);
	chunk->offset = offset;
	chunk->size = size;
	chunk->refs = 1;
	chunk->modifver = cynk.modifver;
	sshfs_send_async_read(sf, chunk);
	pthread_mutex_lock(&cynk.lock);
	chunk_put(*chunkp);
	*chunkp = chunk;
	pthread_mutex_unlock(&cynk.lock);
}

static int wait_chunk(struct read_chunk *chunk, char *buf, size_t size)
{
	int res;
	while (sem_wait(&chunk->ready));
	res = chunk->res;
	if (res > 0) {
		if ((size_t) res > size)
			res = size;
		buf_get_mem(&chunk->data, buf, res);
		chunk->offset += res;
		chunk->size -= res;
		chunk->res -= res;
	}
	sem_post(&chunk->ready);
	chunk_put_locked(chunk);
	return res;
}

static struct read_chunk *search_read_chunk(struct sshfs_file *sf, off_t offset)
{
	struct read_chunk *ch = sf->readahead;
	if (ch && ch->offset == offset && ch->modifver == cynk.modifver) {
		ch->refs++;
		return ch;
	} else
		return NULL;
}

static int sshfs_async_read(struct sshfs_file *sf, char *rbuf, size_t size,
                            off_t offset)
{
	int res = 0;
	size_t total = 0;
	struct read_chunk *chunk;
	struct read_chunk *chunk_prev = NULL;
	size_t origsize = size;
	int curr_is_seq;

	pthread_mutex_lock(&cynk.lock);
	curr_is_seq = sf->is_seq;
	sf->is_seq = (sf->next_pos == offset && sf->modifver == cynk.modifver);
	sf->next_pos = offset + size;
	sf->modifver = cynk.modifver;
	chunk = search_read_chunk(sf, offset);
	pthread_mutex_unlock(&cynk.lock);

	if (chunk && chunk->size < size) {
		chunk_prev = chunk;
		size -= chunk->size;
		offset += chunk->size;
		chunk = NULL;
	}

	if (!chunk)
		submit_read(sf, size, offset, &chunk);

	if (curr_is_seq && chunk && chunk->size <= size)
		submit_read(sf, origsize, offset + size, &sf->readahead);

	if (chunk_prev) {
		size_t prev_size = chunk_prev->size;
		res = wait_chunk(chunk_prev, rbuf, prev_size);
		if (res < (int) prev_size) {
			chunk_put_locked(chunk);
			return res;
		}
		rbuf += res;
		total += res;
	}
	res = wait_chunk(chunk, rbuf, size);
	if (res > 0)
		total += res;
	if (res < 0)
		return res;

	return total;
}

static int sshfs_read(const char *path, char *rbuf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
	struct sshfs_file *sf = get_sshfs_file(fi);
	(void) path;

	if (!sshfs_file_is_conn(sf))
		return -EIO;

	if (cynk.sync_read)
		return sshfs_sync_read(sf, rbuf, size, offset);
	else
		return sshfs_async_read(sf, rbuf, size, offset);
}

static void sshfs_write_begin(struct request *req)
{
	struct sshfs_file *sf = (struct sshfs_file *) req->data;

	sshfs_file_get(sf);
	list_add(&req->list, &sf->write_reqs);
}

static void sshfs_write_end(struct request *req)
{
	uint32_t serr;
	struct sshfs_file *sf = (struct sshfs_file *) req->data;

	if (req->error)
		sf->write_error = req->error;
	else if (req->replied) {
		if (req->reply_type != SSH_FXP_STATUS) {
			fprintf(stderr, "protocol error\n");
		} else if (buf_get_uint32(&req->reply, &serr) != -1 &&
			 serr != SSH_FX_OK) {
			sf->write_error = -EIO;
		}
	}
	list_del(&req->list);
	pthread_cond_broadcast(&sf->write_finished);
	sshfs_file_put(sf);
}

static int sshfs_write(const char *path, const char *wbuf, size_t size,
                       off_t offset, struct fuse_file_info *fi)
{
	int err;
	struct buffer buf;
	struct sshfs_file *sf = get_sshfs_file(fi);
	struct buffer *handle = &sf->handle;
	struct iovec iov[2];

	(void) path;

	if (!sshfs_file_is_conn(sf))
		return -EIO;

	cynk.modifver ++;
	buf_init(&buf, 0);
	buf_add_buf(&buf, handle);
	buf_add_uint64(&buf, offset);
	buf_add_uint32(&buf, size);
	buf_to_iov(&buf, &iov[0]);
	iov[1].iov_base = (void *) wbuf;
	iov[1].iov_len = size;
	if (!cynk.sync_write && !sf->write_error) {
		err = sftp_request_send(SSH_FXP_WRITE, iov, 2,
					sshfs_write_begin, sshfs_write_end,
					0, sf, NULL);
	} else {
		err = sftp_request_iov(SSH_FXP_WRITE, iov, 2, SSH_FXP_STATUS,
				       NULL);
	}
	buf_free(&buf);
	return err ? err : (int) size;
}

static int sshfs_ext_statvfs(const char *path, struct statvfs *stbuf)
{
	int err;
	struct buffer buf;
	struct buffer outbuf;
	buf_init(&buf, 0);
	buf_add_string(&buf, SFTP_EXT_STATVFS);
	buf_add_path(&buf, path);
	err = sftp_request(SSH_FXP_EXTENDED, &buf, SSH_FXP_EXTENDED_REPLY,
			   &outbuf);
	if (!err) {
		if (buf_get_statvfs(&outbuf, stbuf) == -1)
			err = -EIO;
		buf_free(&outbuf);
	}
	buf_free(&buf);
	return err;
}


#if FUSE_VERSION >= 25
static int sshfs_statfs(const char *path, struct statvfs *buf)
{
	if (cynk.ext_statvfs)
		return sshfs_ext_statvfs(path, buf);

	buf->f_namemax = 255;
	buf->f_bsize = cynk.blksize;
	/*
	 * df seems to use f_bsize instead of f_frsize, so make them
	 * the same
	 */
	buf->f_frsize = buf->f_bsize;
	buf->f_blocks = buf->f_bfree =  buf->f_bavail =
		1000ULL * 1024 * 1024 * 1024 / buf->f_frsize;
	buf->f_files = buf->f_ffree = 1000000000;
	return 0;
}
#else
static int sshfs_statfs(const char *path, struct statfs *buf)
{
	if (cynk.ext_statvfs) {
		int err;
		struct statvfs vbuf;

		err = sshfs_ext_statvfs(path, &vbuf);
		if (!err) {
			buf->f_bsize = vbuf.f_bsize;
			buf->f_blocks = vbuf.f_blocks;
			buf->f_bfree = vbuf.f_bfree;
			buf->f_bavail = vbuf.f_bavail;
			buf->f_files = vbuf.f_files;
			buf->f_ffree = vbuf.f_ffree;
			buf->f_namelen = vbuf.f_namemax;
		}
		return err;
	}

	buf->f_namelen = 255;
	buf->f_bsize = cynk.blksize;
	buf->f_blocks = buf->f_bfree = buf->f_bavail =
		1000ULL * 1024 * 1024 * 1024 / buf->f_bsize;
	buf->f_files = buf->f_ffree = 1000000000;
	return 0;
}
#endif

#if FUSE_VERSION >= 25
static int sshfs_create(const char *path, mode_t mode,
                        struct fuse_file_info *fi)
{
	return sshfs_open_common(path, mode, fi);
}

static int sshfs_ftruncate(const char *path, off_t size,
                           struct fuse_file_info *fi)
{
	int err;
	struct buffer buf;
	struct sshfs_file *sf = get_sshfs_file(fi);

	(void) path;

	if (!sshfs_file_is_conn(sf))
		return -EIO;

	cynk.modifver ++;
	if (cynk.truncate_workaround)
		return sshfs_truncate_workaround(path, size, fi);

	buf_init(&buf, 0);
	buf_add_buf(&buf, &sf->handle);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_SIZE);
	buf_add_uint64(&buf, size);
	err = sftp_request(SSH_FXP_FSETSTAT, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);

	return err;
}
#endif

static int sshfs_fgetattr(const char *path, struct stat *stbuf,
			  struct fuse_file_info *fi)
{
	int err;
	struct buffer buf;
	struct buffer outbuf;
	struct sshfs_file *sf = get_sshfs_file(fi);

	(void) path;

	if (!sshfs_file_is_conn(sf))
		return -EIO;

	buf_init(&buf, 0);
	buf_add_buf(&buf, &sf->handle);
	err = sftp_request(SSH_FXP_FSTAT, &buf, SSH_FXP_ATTRS, &outbuf);
	if (!err) {
		if (buf_get_attrs(&outbuf, stbuf, NULL) == -1)
			err = -EIO;
		buf_free(&outbuf);
	}
	buf_free(&buf);
	return err;
}

static int sshfs_truncate_zero(const char *path)
{
	int err;
	struct fuse_file_info fi;

	fi.flags = O_WRONLY | O_TRUNC;
	err = sshfs_open(path, &fi);
	if (!err)
		sshfs_release(path, &fi);

	return err;
}

static size_t calc_buf_size(off_t size, off_t offset)
{
	return offset + cynk.max_read < size ? cynk.max_read : size - offset;
}

static int sshfs_truncate_shrink(const char *path, off_t size)
{
	int res;
	char *data;
	off_t offset;
	struct fuse_file_info fi;

	data = calloc(size, 1);
	if (!data)
		return -ENOMEM;

	fi.flags = O_RDONLY;
	res = sshfs_open(path, &fi);
	if (res)
		goto out;

	for (offset = 0; offset < size; offset += res) {
		size_t bufsize = calc_buf_size(size, offset);
		res = sshfs_read(path, data + offset, bufsize, offset, &fi);
		if (res <= 0)
			break;
	}
	sshfs_release(path, &fi);
	if (res < 0)
		goto out;

	fi.flags = O_WRONLY | O_TRUNC;
	res = sshfs_open(path, &fi);
	if (res)
		goto out;

	for (offset = 0; offset < size; offset += res) {
		size_t bufsize = calc_buf_size(size, offset);
		res = sshfs_write(path, data + offset, bufsize, offset, &fi);
		if (res < 0)
			break;
	}
	if (res >= 0)
		res = sshfs_flush(path, &fi);
	sshfs_release(path, &fi);

out:
	free(data);
	return res;
}

static int sshfs_truncate_extend(const char *path, off_t size,
                                 struct fuse_file_info *fi)
{
	int res;
	char c = 0;
	struct fuse_file_info tmpfi;
	struct fuse_file_info *openfi = fi;
	if (!fi) {
		openfi = &tmpfi;
		openfi->flags = O_WRONLY;
		res = sshfs_open(path, openfi);
		if (res)
			return res;
	}
	res = sshfs_write(path, &c, 1, size - 1, openfi);
	if (res == 1)
		res = sshfs_flush(path, openfi);
	if (!fi)
		sshfs_release(path, openfi);

	return res;
}


/*
 * Work around broken sftp servers which don't handle
 * SSH_FILEXFER_ATTR_SIZE in SETSTAT request.
 *
 * If new size is zero, just open the file with O_TRUNC.
 *
 * If new size is smaller than current size, then copy file locally,
 * then open/trunc and send it back.
 *
 * If new size is greater than current size, then write a zero byte to
 * the new end of the file.
 */
static int sshfs_truncate_workaround(const char *path, off_t size,
                                     struct fuse_file_info *fi)
{
	if (size == 0)
		return sshfs_truncate_zero(path);
	else {
		struct stat stbuf;
		int err;
		if (fi)
			err = sshfs_fgetattr(path, &stbuf, fi);
		else
			err = sshfs_getattr(path, &stbuf);
		if (err)
			return err;
		if (stbuf.st_size == size)
			return 0;
		else if (stbuf.st_size > size)
			return sshfs_truncate_shrink(path, size);
		else
			return sshfs_truncate_extend(path, size, fi);
	}
}

static int processing_init(void)
{
	signal(SIGPIPE, SIG_IGN);

	pthread_mutex_init(&cynk.lock, NULL);
	pthread_mutex_init(&cynk.lock_write, NULL);
	pthread_cond_init(&cynk.outstanding_cond, NULL);
	cynk.reqtab = g_hash_table_new(NULL, NULL);
	if (!cynk.reqtab) {
		fprintf(stderr, "failed to create hash table\n");
		return -1;
	}
	return 0;
}



static int is_ssh_opt(const char *arg)
{
	if (arg[0] != '-') {
		unsigned arglen = strlen(arg);
		const char **o;
		for (o = ssh_opts; *o; o++) {
			unsigned olen = strlen(*o);
			if (arglen > olen && arg[olen] == '=' &&
			    strncasecmp(arg, *o, olen) == 0)
				return 1;
		}
	}
	return 0;
}


static int workaround_opt_proc(void *data, const char *arg, int key,
			       struct fuse_args *outargs)
{
	(void) data; (void) key; (void) outargs;
	fprintf(stderr, "unknown workaround: '%s'\n", arg);
	return -1;
}

int parse_workarounds(void)
{
	int res;
	char *argv[] = { "", "-o", cynk.workarounds, NULL };
	struct fuse_args args = FUSE_ARGS_INIT(3, argv);
	char *s = cynk.workarounds;
	if (!s)
		return 0;

	while ((s = strchr(s, ':')))
		*s = ',';

	res = fuse_opt_parse(&args, &cynk, workaround_opts,
			     workaround_opt_proc);
	fuse_opt_free_args(&args);

	return res;
}

#if FUSE_VERSION == 25
static int fuse_opt_insert_arg(struct fuse_args *args, int pos,
                               const char *arg)
{
	assert(pos <= args->argc);
	if (fuse_opt_add_arg(args, arg) == -1)
		return -1;

	if (pos != args->argc - 1) {
		char *newarg = args->argv[args->argc - 1];
		memmove(&args->argv[pos + 1], &args->argv[pos],
			sizeof(char *) * (args->argc - pos - 1));
		args->argv[pos] = newarg;
	}
	return 0;
}
#endif

static void check_large_read(struct fuse_args *args)
{
	struct utsname buf;
	int err = uname(&buf);
	if (!err && strcmp(buf.sysname, "Linux") == 0 &&
	    strncmp(buf.release, "2.4.", 4) == 0)
		fuse_opt_insert_arg(args, 1, "-olarge_read");
}

static int read_password(void)
{
	int size = getpagesize();
	int max_password = 64;
	int n;

	cynk.password = mmap(NULL, size, PROT_READ | PROT_WRITE,
			      MAP_PRIVATE | MAP_ANONYMOUS | MAP_LOCKED,
			      -1, 0);
	if (cynk.password == MAP_FAILED) {
		perror("Failed to allocate locked page for password");
		return -1;
	}

	/* Don't use fgets() because password might stay in memory */
	for (n = 0; n < max_password; n++) {
		int res;

		res = read(0, &cynk.password[n], 1);
		if (res == -1) {
			perror("Reading password");
			return -1;
		}
		if (res == 0) {
			cynk.password[n] = '\n';
			break;
		}
		if (cynk.password[n] == '\n')
			break;
	}
	if (n == max_password) {
		fprintf(stderr, "Password too long\n");
		return -1;
	}
	cynk.password[n+1] = '\0';
	ssh_add_arg("-oNumberOfPasswordPrompts=1");
	ssh_add_arg("-oPreferredAuthentications=password,keyboard-interactive");

	return 0;
}

static void set_ssh_command(void)
{
	char *s;
	char *d;
	int i = 0;
	int end = 0;

	d = cynk.ssh_command;
	s = cynk.ssh_command;
	while (!end) {
		switch (*s) {
		case '\0':
			end = 1;
		case ' ':
			*d = '\0';
			if (i == 0) {
				replace_arg(&cynk.ssh_args.argv[0],
					    cynk.ssh_command);
			} else {
				if (fuse_opt_insert_arg(&cynk.ssh_args, i, 
						cynk.ssh_command) == -1)
					_exit(1);
			}
			i++;
			d = cynk.ssh_command;
			break;

		case '\\':
			if (s[1])
				s++;
		default:
			*d++ = *s;
		}
		s++;
	}
}

static char *find_base_path(void)
{
	char *s = cynk.host;
	char *d = s;

	for (; *s && *s != ':'; s++) {
		if (*s == '[') {
			/*
			 * Handle IPv6 numerical address enclosed in square
			 * brackets
			 */
			s++;
			for (; *s != ']'; s++) {
				if (!*s) {
					fprintf(stderr,	"missing ']' in hostname\n");
					exit(1);
				}
				*d++ = *s;
			}
		} else {
			*d++ = *s;
		}

	}
	*d++ = '\0';
	s++;

	return s;
}

/*
 * Remove commas from fsname, as it confuses the fuse option parser.
 */
static void fsname_remove_commas(char *fsname)
{
	if (strchr(fsname, ',') != NULL) {
		char *s = fsname;
		char *d = s;

		for (; *s; s++) {
			if (*s != ',')
				*d++ = *s;
		}
		*d = *s;
	}
}

#if FUSE_VERSION >= 27
static char *fsname_escape_commas(char *fsnameold)
{
	char *fsname = g_malloc(strlen(fsnameold) * 2 + 1);
	char *d = fsname;
	char *s;

	for (s = fsnameold; *s; s++) {
		if (*s == '\\' || *s == ',')
			*d++ = '\\';
		*d++ = *s;
	}
	*d = '\0';
	g_free(fsnameold);

	return fsname;
}
#endif

static char * rand_str(size_t len)
{
	char *s = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	int l = strlen(s);
	char *str = g_malloc0(len + 1);
	size_t i;
	
	srand(time(NULL));
	for (i = 0; i < len; i++)
		str[i] = s[rand() % l];

	return str;
}

static double tv_get_elapsed(struct timeval *end, struct timeval *start)
{
	struct timeval res;

	if (end->tv_usec < start->tv_usec) {
		int nsec = (start->tv_usec - end->tv_usec) / 1000000 + 1;
		start->tv_usec -= 1000000 * nsec;
		start->tv_sec += nsec;
	}
	if (end->tv_usec - start->tv_usec > 1000000) {
		int nsec = (end->tv_usec - start->tv_usec) / 1000000;
		start->tv_usec += 1000000 * nsec;
		start->tv_sec -= nsec;
	}
	res.tv_sec = end->tv_sec - start->tv_sec;
	res.tv_usec = end->tv_usec - start->tv_usec;

	return (double) res.tv_sec + ((double) res.tv_usec / 1000000);
}

static double ts_get_elapsed(struct timespec *end, struct timespec *start)
{
	struct timespec res;

	if (end->tv_nsec < start->tv_nsec) {
		int nsec = (start->tv_nsec - end->tv_nsec) / 1000000000 + 1;
		start->tv_nsec -= 1000000000 * nsec;
		start->tv_sec += nsec;
	}
	if (end->tv_nsec - start->tv_nsec > 1000000000) {
		int nsec = (end->tv_nsec - start->tv_nsec) / 1000000000;
		start->tv_nsec += 1000000000 * nsec;
		start->tv_sec -= nsec;
	}
	res.tv_sec = end->tv_sec - start->tv_sec;
	res.tv_nsec = end->tv_nsec - start->tv_nsec;

	return (double) res.tv_sec + ((double) res.tv_nsec / 1000000000);
}

static char * time_str(time_t *now)
{
	struct tm *local;
	char *tm_str;
		
	local = localtime(now);
	tm_str = asctime(local);
	tm_str[strlen(tm_str) - 1] = '\0';

	return tm_str;
}

static void verbose(int index, const char *template, ...)
{
	va_list ap;
	
	if (index) {
		fprintf(stdout, "[%05d] ", cynk.verbose_cnt);
		cynk.verbose_cnt++;
	} else
		fprintf(stdout, "  ");
	va_start(ap, template);
	vfprintf(stdout, template, ap);
	va_end(ap);
	fprintf(stdout, "\n");
	fflush(stdout);
}

static int run(const char *format, ...)
{
	va_list argv;
	FILE *fp;
	char *command;
	char line[2048];
	int res = 0;

	va_start(argv, format);
	command = g_strdup_vprintf(format, argv);
	va_end(argv);

	if (cynk.verbosity >= 3)
		verbose(FALSE, "> %s", command);
	if (!cynk.dryrun) {
		if (!(fp = popen(command, "r")))
			fatal(1, "failed to run command %s", command);
		if (cynk.verbosity >= 3) {
			while (fgets(line, sizeof(line), fp))
				fprintf(stdout, "    %s", line);
		}
		res = pclose(fp);
	}
	return res;
}

static char * rsync_common_opts(void)
{
	char opts[16];
	int index = 0;
	
	memset(opts, 0, sizeof(opts));
	opts[index++] = '-';
	opts[index++] = 'a';
	opts[index++] = 'u';
	if (cynk.verbosity >= 3)
		opts[index++] = 'i';
	if (cynk.dryrun)
		opts[index++] = 'n';

	return g_strdup(opts);
}

static void on_born(const char *path, uint32_t dir)
{
	if (g_hash_table_lookup(cynk.dead, path)) {
		if (cynk.verbosity >= 3)
			verbose(FALSE, "dead: - %s", path);
		pthread_mutex_lock(&cynk.dead_lock);
		g_hash_table_remove(cynk.dead, path);
		pthread_mutex_unlock(&cynk.dead_lock);
	} else {
		if (cynk.verbosity >= 3)
			verbose(FALSE, "born: + %s", path);
		char *key = g_strdup(path);
		pthread_mutex_lock(&cynk.born_lock);
		g_hash_table_insert(cynk.born, key, GUINT_TO_POINTER(dir));
		pthread_mutex_unlock(&cynk.born_lock);
	}
}

static void on_dead(const char *path, uint32_t dir)
{
	if (g_hash_table_lookup(cynk.born, path)) {
		if (cynk.verbosity >= 3)
			verbose(FALSE, "born: - %s", path);
		pthread_mutex_lock(&cynk.born_lock);
		g_hash_table_remove(cynk.born, path);
		pthread_mutex_unlock(&cynk.born_lock);
	} else {
		if (cynk.verbosity >= 3)
			verbose(FALSE, "dead: + %s", path);
		char *key = g_strdup(path);
		pthread_mutex_lock(&cynk.dead_lock);
		g_hash_table_insert(cynk.dead, key, GUINT_TO_POINTER(dir));
		pthread_mutex_unlock(&cynk.dead_lock);
	}
}
#ifndef GLIB_HASH_TABLE_HAS_ITER
struct write_line_data {
	FILE *fp;
};

static void write_line(gpointer key, gpointer value, gpointer data)
{
	struct write_line_data *p = (struct write_line_data *) data;
	fprintf(p->fp, "- %s\n", (char *) key);
}
#endif

static int make_pull_exclude(const char *path)
{
	FILE *fp;

	fp = fopen(path, "w+");
	if (fp == NULL) {
		perror2("failed to open file \"%s\"", path);
		return -1;
	}

	if (cynk.verbosity >= 3)
		verbose(FALSE, "writing exclude rules to %s", path);

#ifdef GLIB_HASH_TABLE_HAS_ITER
	GHashTableIter iter;
	gpointer key, value;

	fprintf(fp, "# Born files\n");
	g_hash_table_iter_init(&iter, cynk.born);
	while (g_hash_table_iter_next(&iter, &key, &value))
		fprintf(fp, "- %s\n", (char *) key);
	
	fprintf(fp, "# Dead files\n");
	g_hash_table_iter_init(&iter, cynk.dead);
	while (g_hash_table_iter_next(&iter, &key, &value))
		fprintf(fp, "- %s\n", (char *) key);
#else
	struct write_line_data data;
	data.fp = fp;
	
	fprintf(fp, "# Born files\n");
	g_hash_table_foreach(cynk.born, write_line, &data);
	
	fprintf(fp, "# Dead files\n");
	g_hash_table_foreach(cynk.dead, write_line, &data);
#endif
	fclose(fp);
	return 0;
}

static int make_push_include(const char *path)
{
	FILE *fp;
	
	fp = fopen(path, "w+");
	if (fp == NULL) {
		perror2("failed to open file \"%s\"", path);
		return -1;
	}

	if (cynk.verbosity >= 3)
		verbose(FALSE, "writing include rules to %s", path);

#ifdef GLIB_HASH_TABLE_HAS_ITER
	GHashTableIter iter;
	gpointer key, value;
	
	fprintf(fp, "# Dead files\n");
	g_hash_table_iter_init(&iter, cynk.dead);
	while (g_hash_table_iter_next(&iter, &key, &value))
		fprintf(fp, "+ %s\n", (char *) key);
#else
	struct write_line_data data;
	data.fp = fp;
	
	fprintf(fp, "# Dead files\n");
	g_hash_table_foreach(cynk.dead, write_line, &data);
#endif

	fclose(fp);
	return 0;
}

static double synchronize(void)
{	
	int res;
	time_t start, end;
	struct timeval tv_start, tv_end;
	double elapsed;
	
	pthread_mutex_lock(&cynk.sync_lock);
	cynk.sync_in_progress = 1;
	start = time(NULL);
	if (cynk.verbosity >= 1)
		verbose(FALSE, "---------------- SYNC START at %s ----------------", 
			time_str(&start));
	
	if (cynk.verbosity >= 2)
		verbose(FALSE, "PULL phase starting...");
	gettimeofday(&tv_start, NULL);
	make_pull_exclude(cynk.exclude_file);
	res = run("rsync %s/ %s/ --exclude=/%s --temp-dir=%s "
		"--delete --exclude-from=%s %s",
		cynk.remote, cynk.local, REPO_HOME, TRANS_HOME,
		cynk.exclude_file, cynk.rsync_common_opts);
	
	/* prevent very recently created files from being deleted on remote */
	if (cynk.verbosity >= 2)
		verbose(FALSE, "PUSH phase starting...");
	make_push_include(cynk.include_file);
	res = run("rsync %s/ %s/ --exclude=/%s --temp-dir=%s "
		"--delete --include-from=%s --filter='P /*' %s",
		cynk.local, cynk.remote, REPO_HOME, TRANS_HOME,
		cynk.include_file, cynk.rsync_common_opts);
	gettimeofday(&tv_end, NULL);
	
	pthread_mutex_lock(&cynk.born_lock);
	g_hash_table_remove_all(cynk.born);
	pthread_mutex_unlock(&cynk.born_lock);
	
	pthread_mutex_lock(&cynk.dead_lock);
	g_hash_table_remove_all(cynk.dead);
	pthread_mutex_unlock(&cynk.dead_lock);
	
	elapsed = tv_get_elapsed(&tv_end, &tv_start);
	
	end = time(NULL);
	if (cynk.verbosity >= 1)
		verbose(FALSE, 
			"------- SYNC END at %s, took %.3f seconds -------", 
			time_str(&end), elapsed);
	cynk.sync_in_progress = 0;

	pthread_mutex_lock(&cynk.block_lock);
	pthread_cond_broadcast(&cynk.block_cond);
	pthread_mutex_unlock(&cynk.block_lock);

	pthread_mutex_unlock(&cynk.sync_lock);

	return elapsed;
}


static void *process_sync(void *data)
{
	(void) data;
	struct timespec now, wait;
	double timeout;
	
	while (1) {
		pthread_mutex_lock(&cynk.sync_thread_stop_lock);
		clock_gettime(CLOCK_REALTIME, &now);
		wait = now;
		wait.tv_sec += cynk.duration;
		pthread_cond_timedwait(&cynk.sync_thread_stop_cond, 
			&cynk.sync_thread_stop_lock, &wait);
		if (cynk.sync_thread_stop)
			break;
		timeout = ts_get_elapsed(&now, &cynk.sysc_stamp);
		if (timeout > cynk.window) {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "SYNC: no system call since %.3f secs ago",
					timeout); 
			synchronize();
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, 
					"REST: system call detected %.3f seconds ago",
					timeout); 
		}
		pthread_mutex_unlock(&cynk.sync_thread_stop_lock);
	}

	pthread_exit(NULL);
}

static int start_sync_thread(void)
{
	int err;
	
	if (cynk.verbosity >= 3)
		verbose(TRUE, "START SYNC THREAD: duration=%d, window=%d", 
			cynk.duration, cynk.window);

	pthread_mutex_init(&cynk.sync_lock, NULL);
	pthread_attr_init(&cynk.sync_thread_attr);
	pthread_mutex_init(&cynk.sync_thread_stop_lock, NULL);
	pthread_cond_init(&cynk.sync_thread_stop_cond, NULL);
	
	pthread_mutex_init(&cynk.block_lock, NULL);
	pthread_cond_init(&cynk.block_cond, NULL);

	pthread_attr_setdetachstate(&cynk.sync_thread_attr,
		PTHREAD_CREATE_JOINABLE);
	err = pthread_create(&cynk.sync_thread, NULL, process_sync, NULL);
	if (err) {
		error2(err, "create thread");
		return -EIO;
	}
	pthread_detach(cynk.sync_thread);
	return 0;
}

static void stop_sync_thread(void)
{
	if (cynk.verbosity >= 2)
		verbose(FALSE, "* Stop sync thread ...");
	
	pthread_mutex_lock(&cynk.sync_thread_stop_lock);
	cynk.sync_thread_stop = 1;
	pthread_cond_signal(&cynk.sync_thread_stop_cond);
	pthread_mutex_unlock(&cynk.sync_thread_stop_lock);
	pthread_join(cynk.sync_thread, NULL);
	
	pthread_attr_destroy(&cynk.sync_thread_attr);
	pthread_mutex_destroy(&cynk.sync_thread_stop_lock);
	pthread_cond_destroy(&cynk.sync_thread_stop_cond);
	pthread_mutex_destroy(&cynk.block_lock);
	pthread_cond_destroy(&cynk.block_cond);
}

static inline void actions_before(void)
{
	/* block when sync in progress */
	if (cynk.block && cynk.sync_in_progress) {
		pthread_mutex_lock(&cynk.block_lock);
		pthread_cond_wait(&cynk.block_cond, &cynk.block_lock);
		pthread_mutex_unlock(&cynk.block_lock);
	}
	clock_gettime(CLOCK_REALTIME, &cynk.sysc_stamp);
}

enum {
	ENTRY_IS_FILE = 1,
	ENTRY_IS_DIR = 2,
	ENTRY_IS_SYMLINK = 3,
	ENTRY_IS_LINK = 4,
};

/*
 * FUSE APIs
 */
static int cynk_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;

	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
		return sshfs_oper.oper.getattr(path, stbuf);
	} else if (cynk.rsync_on) {
		char *realpath = add_path(path);
		actions_before();
		res = lstat(realpath, stbuf);
		g_free(realpath);
	}	
	if (res == -1)
		return -errno;
	return 0;
}

static int cynk_fgetattr(const char *path, struct stat *stbuf,
			struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
#if FUSE_VERSION >= 25
		return sshfs_oper.oper.fgetattr(path, stbuf, fi);
#endif
	}	
	else if (cynk.rsync_on) {	
		int res;
		(void) path;
	
		actions_before();

		res = fstat(fi->fh, stbuf);
		if (res == -1)
			return -errno;		
	}
	return 0;
}

static int cynk_access(const char *path, int mask)
{
	if (cynk.sshfs_on && is_online(path)) {
		struct stat stbuf;
		int err;

		err = sshfs_oper.oper.getattr(path, &stbuf);
		if (err)
			return err;

		if (mask & R_OK) {
		 	if (cynk.uid == stbuf.st_uid) {
				if (!(stbuf.st_mode & S_IRUSR))
					return EACCES;
			}
			else if (cynk.gid == stbuf.st_gid) {
				if (!(stbuf.st_mode & S_IRGRP))
					return EACCES;
			}
			else if (!(stbuf.st_mode & S_IROTH)) {
					return EACCES;
			}
		}
		if (mask & W_OK) {
			if (cynk.uid == stbuf.st_uid) {
				if (!(stbuf.st_mode & S_IWUSR))
					return EACCES;
			}
			else if (cynk.gid == stbuf.st_gid) {
				if (!(stbuf.st_mode & S_IWGRP))
					return EACCES;
			}
			else {
				if (!(stbuf.st_mode & S_IWOTH))
					return EACCES;
			}
		}
		if (mask & X_OK) {
			if (cynk.uid == stbuf.st_uid) {
				if (!(stbuf.st_mode & S_IXUSR))
					return EACCES;
			}
			else if (cynk.gid == stbuf.st_gid) {
				if (!(stbuf.st_mode & S_IXGRP))
					return EACCES;
			}
			else if (!(stbuf.st_mode & S_IXOTH)) {
					return EACCES;
			}
		}
	} else if (cynk.rsync_on) {	
		int res;
		char *realpath = add_path(path);

		actions_before();

		res = access(realpath, mask);

		g_free(realpath);
		if (res == -1)
			return -errno;
	}
	return 0;
}

static int cynk_readlink(const char *path, char *buf, size_t size)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.readlink(path,buf,size);	
	else if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(path);
	
		actions_before();

		res = readlink(realpath, buf, size - 1);
	
		g_free(realpath);
		if (res == -1)
			return -errno;

		buf[res] = '\0';
	}
	return 0;
}

static inline struct cynk_dir *get_dirp(struct fuse_file_info *fi)
{
	return (struct cynk_dir *) (uintptr_t) fi->fh;
}

static int sshfs_opendir(const char *path, struct fuse_file_info *fi)
{
	int err;
	struct buffer buf;
	struct buffer *handle;
	struct cynk_dir *d = get_dirp(fi);

	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	handle = malloc(sizeof(struct buffer));
	if (handle == NULL)
		return -ENOMEM;

	err = sftp_request(SSH_FXP_OPENDIR, &buf, SSH_FXP_HANDLE, handle);
	if (!err)
		d->handle = handle;
	else
		free(handle);
	buf_free(&buf);	
	return err;
}

static int cynk_opendir(const char *path, struct fuse_file_info *fi)
{
	struct cynk_dir *d;
	
	d = malloc(sizeof(struct cynk_dir));
	if (d == NULL)
		return -ENOMEM;
	memset(d, 0, sizeof(struct cynk_dir));
	fi->fh = (unsigned long) d;

	if (cynk.sshfs_on && is_online(path)) {
		d->online = 1;
		return sshfs_oper.oper.opendir(path, fi);
	}
	
	if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(path);
		
		actions_before();

		d->dp = opendir(realpath);

		g_free(realpath);
		if (d->dp == NULL) {
			res = -errno;
			free(d);
			return res;
		}
		d->offset = 0;
		d->entry = NULL;
	}
	return 0;
}

static int sshfs_readdir(const char *path, void *buf,	
	fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
	int err;
	struct cynk_dir *d = get_dirp(fi);
	(void) path;
	(void) offset;

	buf_finish(d->handle);
	do {
		struct buffer name;
		err = sftp_request(SSH_FXP_READDIR, d->handle, SSH_FXP_NAME, &name);
		if (!err) {
			if (buf_get_entries(path, &name, buf, filler, d) == -1)
				err = -EIO;
			buf_free(&name);
		}
	} while (!err);
	if (err == MY_EOF)
		err = 0;
	return err;
}

static int cynk_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	struct cynk_dir *d = get_dirp(fi);

	if (cynk.sshfs_on && d->online)
		return sshfs_oper.oper.readdir(path, buf, filler, offset, fi);

	if (cynk.rsync_on) {
		struct cynk_dir *d = get_dirp(fi);
		actions_before();

		if (offset != d->offset) {
			seekdir(d->dp, offset);
			d->entry = NULL;
			d->offset = offset;
		}
		while (1) {
			struct stat st;
			off_t nextoff;
	
			if (!d->entry) {
				d->entry = readdir(d->dp);
				if (!d->entry)
					break;
			}

			memset(&st, 0, sizeof(st));
			st.st_ino = d->entry->d_ino;
			st.st_mode = d->entry->d_type << 12;
			nextoff = telldir(d->dp);
			if (filler(buf, d->entry->d_name, &st, nextoff))
				break;

			d->entry = NULL;
			d->offset = nextoff;
		}
	}
	return 0;
}

static int sshfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	int err;
	struct cynk_dir *d = get_dirp(fi);
	(void) path;

	err = sftp_request(SSH_FXP_CLOSE, d->handle, 0, NULL);

	buf_free(d->handle);
	free(d);
	return err;
}

static int cynk_releasedir(const char *path, struct fuse_file_info *fi)
{	
	struct cynk_dir *d = get_dirp(fi);

	if (cynk.sshfs_on && d->online)
		return sshfs_oper.oper.releasedir(path, fi);

	if (cynk.rsync_on) {
		actions_before();
		closedir(d->dp);
		free(d);
	}
	return 0;
}

static int cynk_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res = 0;
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.mknod(path,mode,rdev);	
	else if (cynk.rsync_on) {
		
		char *realpath = add_path(path);
	
		actions_before();

		if (S_ISFIFO(mode))
			res = mkfifo(realpath, mode);
		else
			res = mknod(realpath, mode, rdev);

		if (res == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "MKNOD: %s", realpath);
			on_born(path, ENTRY_IS_FILE); /* use path */
		}
		g_free(realpath);		
	}
	return res;
}

static int cynk_mkdir(const char *path, mode_t mode)
{
	
	int res = 0;
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.mkdir(path, mode);
	else if (cynk.rsync_on) {
		
		char *realpath = add_path(path);
		actions_before();
		if (mkdir(realpath, mode) == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "MKDIR: %s", realpath);
			on_born(path, ENTRY_IS_DIR); /* use path here */
		}	
	g_free(realpath);
	
	}
	return res;
}

static int cynk_unlink(const char *path)
{
	if(cynk.sshfs_on && is_online(path)) {
		return sshfs_oper.oper.unlink(path);
	} else if (cynk.rsync_on) {
		int res = 0;
		char *realpath = add_path(path);
	
		actions_before();
	
		if (unlink(realpath) == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "UNLINK: %s", realpath);
			on_dead(path, ENTRY_IS_FILE); /* use path */
		}	
		g_free(realpath);
		return res;
	}
	return 0;
}

static int cynk_rmdir(const char *path)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.rmdir(path);
	else if (cynk.rsync_on) {	
		int res = 0;
		char *realpath = add_path(path);
	
		actions_before();
	
		if (rmdir(realpath) == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "RMDIR: %s", realpath);
			on_dead(path, ENTRY_IS_DIR); /* use path */
		}
		g_free(realpath);
		return res;
	}
	return 0;
}

static int cynk_symlink(const char *from, const char *to)
{
	//from to problem
	if(cynk.sshfs_on && is_online(from) && is_online(to)) {
		return sshfs_oper.oper.symlink(from, to);
		//	fd
	} else if (cynk.rsync_on && !(is_online(from) || is_online(to))) {	
		int res = 0;
		char *realfrom = add_path(from);
		char *realto = add_path(to);
	
		actions_before();

		res = symlink(realfrom, realto);
		if (res == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "SYMLINK: %s", realto);
			on_born(to, ENTRY_IS_SYMLINK); /* use path */
		}
		g_free(realfrom);
		g_free(realto);
		return res;
	}
	return 0;
}

static int cynk_rename(const char *from, const char *to)
{
	//from to problem
	if(cynk.sshfs_on && is_online(from) && is_online(to)) {
		return sshfs_oper.oper.rename(from, to);
	} else if (cynk.rsync_on && !(is_online(from) || is_online(to))) {
		int res = 0;
		char *realfrom = add_path(from);
		char *realto = add_path(to);
		struct stat stbuf;
		uint32_t dir = ENTRY_IS_FILE;
	
		actions_before();

		lstat(realfrom, &stbuf);
		if (S_ISDIR(stbuf.st_mode)) 
			dir = ENTRY_IS_DIR;

		if (rename(realfrom, realto) == -1) {
			res = -errno;
		} else {
				if (cynk.verbosity >= 3)
					verbose(TRUE, "MOVED_FROM: %s", realfrom);
				on_dead(from, dir); /* use from */
		
			if (cynk.verbosity >= 3)
				verbose(TRUE, "MOVED_TO: %s", realto);
			on_born(to, dir); /* use to */
		}
		g_free(realfrom);
		g_free(realto);
		return res;
	}
	return 0;
}

static int cynk_link(const char *from, const char *to)
{
	//from to problem
	if (cynk.rsync_on && !(is_online(from) || is_online(to))) {
		int res = 0;
		char *realfrom = add_path(from);
		char *realto = add_path(to);
	
		actions_before();
	
		res = link(realfrom, realto);
		if (res == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "LINK: %s", realto);
			on_born(to, ENTRY_IS_LINK); /* use path */
		}
	
		g_free(realfrom);
		g_free(realto);
	}
	return 0;
}

static int cynk_chmod(const char *path, mode_t mode)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))	
		return sshfs_oper.oper.chmod(path, mode);
	else if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(path);
	
		actions_before();

		res = chmod(realpath, mode);

		g_free(realpath);
	
		if (res == -1)
			return -errno;		
	}
	return 0;
}

static int cynk_chown(const char *path, uid_t uid, gid_t gid)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.chown(path, uid,gid);	
	else if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(path);
	
		actions_before();
	
		res = lchown(realpath, uid, gid);

		g_free(realpath);

		if (res == -1)
			return -errno;		
	}
	return 0;
}

static int cynk_truncate(const char *path, off_t size)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.truncate(path, size);
	else if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(path);
	
		actions_before();
	
		res = truncate(realpath, size);
	
		g_free(realpath);
		if (res == -1)
			return -errno;		
	}	
	return 0;
}

static int cynk_ftruncate(const char *path, off_t size,
			 struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)){
#if FUSE_VERSION >= 25
		return sshfs_oper.oper.ftruncate(path,size,fi);	
#endif
	}
	 else if (cynk.rsync_on) {
		int res;
		(void) path;
	
		actions_before();

		res = ftruncate(fi->fh, size);
		if (res == -1)
			return -errno;

	}
	return 0;
}


static int sshfs_utimens(const char *path, const struct timespec ts[2])
{
	int err;
	struct buffer buf;
	struct utimbuf ubuf;
	buf_init(&buf, 0);
	buf_add_path(&buf, path);
	buf_add_uint32(&buf, SSH_FILEXFER_ATTR_ACMODTIME);
	ubuf.actime = ts[0].tv_sec + ts[0].tv_nsec / 1000000000;
	ubuf.modtime = ts[1].tv_sec + ts[1].tv_nsec / 1000000000;
	buf_add_uint32(&buf, ubuf.actime);
	buf_add_uint32(&buf, ubuf.modtime);
	err = sftp_request(SSH_FXP_SETSTAT, &buf, SSH_FXP_STATUS, NULL);
	buf_free(&buf);
	return err;
}

static int cynk_utimens(const char *path, const struct timespec ts[2])
{
	if (cynk.sshfs_on && is_online(path)) {
		return sshfs_oper.oper.utimens(path, ts);
	} else 	if (cynk.rsync_on) {
		int res;
		struct timeval tv[2];
		char *realpath = add_path(path);
		/* WARNING: No utimens() in SSHFS */
		actions_before();

		tv[0].tv_sec = ts[0].tv_sec;
		tv[0].tv_usec = ts[0].tv_nsec / 1000;
		tv[1].tv_sec = ts[1].tv_sec;
		tv[1].tv_usec = ts[1].tv_nsec / 1000;
	
		res = utimes(realpath, tv);
		g_free(realpath);
		if (res == -1)
			return -errno;
	}
	return 0;
}

static int cynk_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{

	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)){
#if FUSE_VERSION >= 25
		return sshfs_oper.oper.create(path, mode, fi);
#endif
	}
	else if (cynk.rsync_on) {
		int fd, res = 0;
		char *realpath = add_path(path);
	
		actions_before();

		fd = open(realpath, fi->flags, mode);
		if (fd == -1) {
			res = -errno;
		} else {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "CREATE: %s", realpath);
			on_born(path, ENTRY_IS_FILE); /* use path */
		}
		fi->fh = fd;
		g_free(realpath);
		return res;
	}
	return 0;
}

static int cynk_open(const char *path, struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
		return sshfs_oper.oper.open(path, fi);
	} else if (cynk.rsync_on) {
		int fd;
		char *realpath = add_path(path);
	
		actions_before();
	
		fd = open(realpath, fi->flags);
		if (fd == -1) {
			g_free(realpath);
			return -errno;
		}
	
		if (fi->flags & O_CREAT) {
			if (cynk.verbosity >= 3)
				verbose(TRUE, "CREAT_ON_OPEN: %s", realpath);
		}

		g_free(realpath);
		fi->fh = fd;
	}
	return 0;
}

static int cynk_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
		return sshfs_oper.oper.read(path, buf, size, offset, fi);
	} else if (cynk.rsync_on) {	
		int res;
		(void) path;
	
		actions_before();
	
		res = pread(fi->fh, buf, size, offset);
		if (res == -1)
			res = -errno;

		return res;
	}
	return 0;
}

static int cynk_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
		return sshfs_oper.oper.write(path, buf, size, offset, fi);
	} else if (cynk.rsync_on) {
		int res;
		(void) path;
	
		actions_before();
	
		res = pwrite(fi->fh, buf, size, offset);
		if (res == -1)
			res = -errno;

		return res;
	}
	return 0;
}

static int cynk_statfs(const char *path, struct statvfs *stbuf)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.statfs(path, stbuf);
	else if (cynk.rsync_on) {	
		int res;
		char *realpath = add_path(path);

		actions_before();
	
		res = statvfs(realpath, stbuf);
		g_free(realpath);
		if (res == -1)
			return -errno;		
	}
	return 0;
}

static int cynk_flush(const char *path, struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
		return sshfs_oper.oper.flush(path, fi);
	} else if (cynk.rsync_on) {	
		int res;
		(void) path;
	
		actions_before();
	
		/* This is called from every close on an open file, so call the
		   close on the underlying filesystem.	But since flush may be
		   called multiple times for an open file, this must not really
		   close the file.  This is important if used on a network
		   filesystem like NFS which flush the data/metadata on close() */
		res = close(dup(fi->fh));
		if (res == -1)
			return -errno;	
	}
	return 0;
}


static int cynk_release(const char *path, struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir)) {
		return sshfs_oper.oper.release(path, fi);
	} else if (cynk.rsync_on) {
		(void) path;
	
		actions_before();
	
		close(fi->fh);
	}
	return 0;
}

static int cynk_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	if(cynk.sshfs_on && g_str_has_prefix(path, cynk.online_dir))
		return sshfs_oper.oper.fsync(path, isdatasync, fi);
	else if (cynk.rsync_on) {
		int res;
		(void) path;
	
		actions_before();

#ifndef HAVE_FDATASYNC
		(void) isdatasync;
#else
		if (isdatasync)
			res = fdatasync(fi->fh);
		else
#endif
			res = fsync(fi->fh);
		if (res == -1)
			return -errno;		
	}
	return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int cynk_setxattr(const char *path, const char *name, const char *value,
			size_t size, int flags)
{
	if (cynk.rsync_on) {	
		int res;
		char *realpath = add_path(path);
	
		actions_before();
	
		res = lsetxattr(path, name, value, size, flags);
		g_free(realpath);
		if (res == -1)
			return -errno;
	}
	return 0;
}

static int cynk_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(realpath);
	
		actions_before();
	
		res = lgetxattr(realpath, name, value, size);
		g_free(realpath);
		if (res == -1)
			return -errno;
		return res;
	}
	return 0;
}

static int cynk_listxattr(const char *path, char *list, size_t size)
{
	if (cynk.rsync_on) {
		int res;
		char *realpath = add_path(realpath);
	
		actions_before();
	
		res = llistxattr(realpath, list, size);
		g_free(realpath);
		if (res == -1)
			return -errno;
		return res;
	}
	return 0;
}

static int cynk_removexattr(const char *path, const char *name)
{
	if (cynk.rsync_on) {	
		int res;
		char *realpath = add_path(realpath);
	
		actions_before();
	
		res = lremovexattr(realpath, name);
		g_free(realpath);
		if (res == -1)
			return -errno;
	}
	return 0;
}
#endif /* HAVE_SETXATTR */

static int cynk_lock(const char *path, struct fuse_file_info *fi, int cmd,
		    struct flock *lock)
{
	if (cynk.rsync_on) {	
		(void) path;
	
		actions_before();

		return ulockmgr_op(fi->fh, cmd, lock, &fi->lock_owner,
			   sizeof(fi->lock_owner));
	}
	return 0;
}

enum {
	CMD_SYNC = 1,
	CMD_CLOSE = 2,
};

static int send_cmd(void)
{
	int sockfd, len;
	struct sockaddr_un addr;
	char buf[MAX_BUF_LEN];

	if ((sockfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		perror2("failed to create socket");
		return -1;
	}
	
	addr.sun_family = AF_UNIX;
	strncpy(addr.sun_path, cynk.cmd_sockpath, sizeof(addr.sun_path));
	len = strlen(addr.sun_path) + sizeof(addr.sun_family);
	if (connect(sockfd, (struct sockaddr *) &addr, len) == -1) {
		perror2("failed to conenct to %s", cynk.cmd_sockpath);
		return -1;
	}
	
	snprintf(buf, MAX_BUF_LEN, "%d", cynk.cmd);
	if (send(sockfd, buf, strlen(buf), 0) == -1) {
		perror2("failed to send");
		return -1;
	}
	
	memset(buf, 0, MAX_BUF_LEN);
	if (recv(sockfd, buf, MAX_BUF_LEN, 0) == -1) {
		perror2("failed to receive");
		return -1;
	}
	fprintf(stdout, "%s\n", buf);
	fflush(stdout);
	return 0;
}

static void *process_cmd(void *data)
{
	int sockfd, cmd;
	socklen_t len;
	struct sockaddr_un addr;
	char buf[MAX_BUF_LEN];
	double elapsed;
	(void) data;

	while (1) {
		sockfd = accept(cynk.cmd_sock, (struct sockaddr *) &addr, &len);
		if (sockfd == -1) {
			perror2("failed to accept socket");
			break;
		}
		
		if (recv(sockfd, buf, MAX_BUF_LEN, 0) == -1) {
			perror2("failed to receive");
			break;
		}
		
		cmd = atoi(buf);
		switch(cmd) {
			case CMD_SYNC:
			if (cynk.verbosity >= 3)
				verbose(TRUE, "COMMAND: SYNC");
				elapsed = synchronize();
				memset(buf, 0, MAX_BUF_LEN);
				snprintf(buf, MAX_BUF_LEN, 
					"Sync with %s done, took %.3f second.",
					cynk.remote, elapsed);
				if (send(sockfd, buf, strlen(buf), 0) == -1)
					perror2("failed to send");
				close(sockfd);
				break;
			default:
				perror2("unknow command code %d");
				break;
		}
	}
	
	pthread_exit(NULL);
}

static int cmd_serv_init(void)
{
	int len, err;
	struct sockaddr_un addr;
	
	if ((cynk.cmd_sock = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
		perror2("failed to create socket");
		return -1;
	}
	
	addr.sun_family = AF_UNIX;
	strncpy(addr.sun_path, cynk.cmd_sockpath, sizeof(addr.sun_path));
	addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';
	unlink(addr.sun_path);
	len = strlen(addr.sun_path) + sizeof(addr.sun_family);
	if (bind(cynk.cmd_sock, (struct sockaddr *)&addr, len) == -1) {
		perror2("failed to bind socket to %s", addr.sun_path);
		return -1;
	}

	if (listen(cynk.cmd_sock, 1) == -1) {
		perror2("failed to listen to socket %d", cynk.cmd_sock);
		return -1;
	}
	
	pthread_attr_setdetachstate(&cynk.cmd_thread_attr,
		PTHREAD_CREATE_JOINABLE);
	err = pthread_create(&cynk.cmd_thread, NULL, process_cmd, NULL);
	if (err) {
		error2(err, "create thread");
		return -EIO;
	}
	pthread_detach(cynk.cmd_thread);
	cynk.cmd_started = 1;

	return 0;
}

static int session_init(void)
{
	FILE *fp;
	char *file;

	/* write gid */
	file = g_strdup_printf("%s/pid", cynk.session);
	fp = fopen(file, "w+");
	if (fp == NULL) {
		perror2("failed to open file \"%s\"", file);
		return -1;
	}
	fprintf(fp, "%d", cynk.pid);
	fclose(fp);
	g_free(file);

	/* write paths */
	file = g_strdup_printf("%s/paths", cynk.session);
	fp = fopen(file, "w+");
	if (fp == NULL) {
		perror2("failed to open file \"%s\"", file);
		return -1;
	}
	fprintf(fp, "local=%s", cynk.mountpoint);
	fprintf(fp, "remote=%s", cynk.remote);
	fprintf(fp, "shadow=%s", cynk.local);
	fclose(fp);
	g_free(file);

	/* start control sock */
	cmd_serv_init();

	return 0;
}

static int cynk_command()
{
	struct stat st;

	cynk.cmd_sockpath = g_strdup_printf("%s/sock", cynk.default_session);
	
	if (lstat(cynk.cmd_sockpath, &st) == -1) {
		if (errno == ENOENT)
			fatal(1, "no cynk session found");
	}
	if (!S_ISSOCK(st.st_mode)) {
		fatal(1, "invalid socket %s\n", cynk.local);
	}

	send_cmd();

	g_free(cynk.username);
	g_free(cynk.userhome);
	g_free(cynk.default_session);
	g_free(cynk.session);
	g_free(cynk.cmd_sockpath);
	
	return 0;
}

enum {
	MODE_AUTO,
	MODE_MANUAL,
};

static void rsync_init(void)
{
	int res;

	cynk.born = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	if (!cynk.born)
		fatal(1, "failed to create hash table");
	
	cynk.dead = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
	if (!cynk.dead)
		fatal(1, "failed to create hash table");

	pthread_mutex_init(&cynk.born_lock, NULL);
	pthread_mutex_init(&cynk.dead_lock, NULL);

	cynk.repo_root = g_strdup_printf("%s/%s", cynk.local, REPO_HOME);
	cynk.trans_root = g_strdup_printf("%s/%s", cynk.local, TRANS_HOME);
	cynk.exclude_file = g_strdup_printf("%s/ex.filter", cynk.repo_root);
	cynk.include_file = g_strdup_printf("%s/ix.filter", cynk.repo_root);
	
	/* initial session */
	if (cynk.verbosity >= 2)
		verbose(FALSE, "* Initial repository...");
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> mkdir %s", cynk.session);
	if (g_mkdir_with_parents(cynk.session, S_IRWXU) == -1)
		fatal(1, "failed to create session directory %s", cynk.session);
	if (unlink(cynk.default_session) == -1 && errno != ENOENT)
		fatal(1, "failed to unlink default session %s", cynk.default_session);
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> ln -s %s %s", cynk.session, cynk.default_session);
	if (symlink(cynk.session, cynk.default_session) == -1)	
		fatal(1, "failed to link default session");

	/* initial repository */
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> mkdir %s", cynk.local);
	if (mkdir(cynk.local, S_IRWXU) == -1 && errno != EEXIST)
		fatal(1, "failed to create shadow repository %s", cynk.local);
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> mkdir %s", cynk.repo_root);
	if (mkdir(cynk.repo_root, S_IRWXU) == -1 && errno != EEXIST)
		fatal(1, "failed to create directory %s", cynk.repo_root);
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> mkdir %s", cynk.trans_root);
	if (mkdir(cynk.trans_root, S_IRWXU) == -1 && errno != EEXIST)
		fatal(1, "failed to create directory %s", cynk.trans_root);

	/* create repository root on remote side */
	cynk.rsync_common_opts = rsync_common_opts();
	res = run("rsync %s %s/ %s", cynk.repo_root, cynk.remote, 
		cynk.rsync_common_opts);

	if (cynk.verbosity >= 1)
		verbose(TRUE, "MIRROR: %s <==> %s", cynk.local, cynk.remote);
	/* two-way sync */
	res = run("rsync %s/ %s/ --exclude=/%s --temp-dir=%s %s",
		cynk.remote, cynk.local, REPO_HOME, TRANS_HOME, 
			cynk.rsync_common_opts);

	res = run("rsync %s/ %s/ --exclude=/%s --temp-dir=%s %s",
		cynk.local, cynk.remote, REPO_HOME, TRANS_HOME, 
			cynk.rsync_common_opts);

	pthread_attr_init(&cynk.cmd_thread_attr);

	session_init();

	if (cynk.mode != MODE_MANUAL)
		start_sync_thread();
}

#if FUSE_VERSION >= 26
static void *cynk_init(struct fuse_conn_info *conn)
#else
static void *cynk_init(void)
#endif
{
	if (cynk.rsync_on)
		rsync_init();

	if (cynk.sshfs_on) {
#ifdef SSHFS_USE_INIT
#if FUSE_VERSION >= 26
		sshfs_init(conn);
#else
		sshfs_init();
#endif
#endif
	}

	return NULL;
}

static void rsync_destroy(void)
{
	int res;

	if (!cynk.rsync_on)
		return;

	if (cynk.mode != MODE_MANUAL)
		stop_sync_thread();
	
	if (cynk.verbosity >= 2)
		verbose(FALSE, "* Stop control server thread ...");
	close(cynk.cmd_sock);
	pthread_join(cynk.cmd_thread, NULL);
	unlink(cynk.cmd_sockpath);
	
	if (cynk.verbosity >= 3)
		verbose(FALSE, "Clean up ...");
	/* cleanup remote repository */
	res = run("rm -rf %s", cynk.repo_root);
	res = run("rsync %s/ %s/ --delete --filter='+ /%s' --filter='- /*' %s",
		cynk.local, cynk.remote, REPO_HOME, cynk.rsync_common_opts);
	
	/* move repository back, safe since FUSE has unmounted */
	if (cynk.verbosity >= 2)
		verbose(FALSE, "* Move back repository ...");
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> rm %s", cynk.mountpoint);
	if (rmdir(cynk.mountpoint) == -1)
		perror2("failed to remove %s", cynk.mountpoint);
	if (cynk.verbosity >= 3)
		verbose(FALSE, "> mv %s %s", cynk.local, cynk.mountpoint);
	if (rename(cynk.local, cynk.mountpoint) == -1)
		perror2("failed to move %s to %s", cynk.local, cynk.mountpoint);
	
	res = run("rm -rf %s", cynk.session);
	
	pthread_mutex_destroy(&cynk.sync_lock);
	pthread_attr_destroy(&cynk.cmd_thread_attr);
	
	pthread_mutex_destroy(&cynk.born_lock);
	pthread_mutex_destroy(&cynk.dead_lock);
	g_hash_table_destroy(cynk.born);
	g_hash_table_destroy(cynk.dead);
	
	g_free(cynk.exclude_file);
	g_free(cynk.include_file);
	g_free(cynk.trans_root);
	g_free(cynk.repo_root);
	g_free(cynk.local);
	g_free(cynk.remote);
}

static void sshfs_destroy(void)
{
	if (!cynk.sshfs_on)
		return;

	unsigned int avg_rtt = 0;
	if (cynk.num_sent)
		avg_rtt = cynk.total_rtt / cynk.num_sent;

	DEBUG("\n"
		  "sent:               %llu messages, %llu bytes\n"
		  "received:           %llu messages, %llu bytes\n"
		  "rtt min/max/avg:    %ums/%ums/%ums\n"
		  "num connect:        %u\n",
		  (unsigned long long) cynk.num_sent,
		  (unsigned long long) cynk.bytes_sent,
		  (unsigned long long) cynk.num_received,
		  (unsigned long long) cynk.bytes_received,
		  cynk.min_rtt, cynk.max_rtt, avg_rtt,
		  cynk.num_connect);
		
	g_free(cynk.online_dir);
}

static void cynk_destroy(void *data_)
{
	(void) data_;
	
	if (cynk.verbosity >= 1)
		verbose(TRUE, "STOP");
	
	if (cynk.rsync_on)	
		rsync_destroy();
	if (cynk.sshfs_on)
		sshfs_destroy();
	
	g_free(cynk.cmd_sockpath);
	g_free(cynk.default_session);
	g_free(cynk.session);
	g_free(cynk.userhome);
	g_free(cynk.username);
	g_free(cynk.rsync_common_opts);
	g_free(cynk.mountpoint);
	
	if (cynk.verbosity >= 3)
		verbose(FALSE, "Bye.");
}

static struct fuse_cache_operations sshfs_oper = {
	.oper = {
#ifdef SSHFS_USE_INIT
		.init		= sshfs_init,
#endif
		.getattr	= sshfs_getattr,
		.readlink	= sshfs_readlink,
		.mknod		= sshfs_mknod,
		.mkdir		= sshfs_mkdir,
		.opendir	= sshfs_opendir,
		.readdir	= sshfs_readdir,
		.releasedir	= sshfs_releasedir,
		.symlink	= sshfs_symlink,
		.unlink		= sshfs_unlink,
		.rmdir		= sshfs_rmdir,
		.rename		= sshfs_rename,
		.chmod		= sshfs_chmod,
		.chown		= sshfs_chown,
		.truncate	= sshfs_truncate,
		.utimens	= sshfs_utimens,
		.open		= sshfs_open,
		.flush		= sshfs_flush,
		.fsync		= sshfs_fsync,
		.release	= sshfs_release,
		.read		= sshfs_read,
		.write		= sshfs_write,
		.statfs		= sshfs_statfs,
#if FUSE_VERSION >= 25
		.create		= sshfs_create,
		.ftruncate	= sshfs_ftruncate,
		.fgetattr	= sshfs_fgetattr,
#endif
	},
};

static struct fuse_operations cynk_oper = {
	.init		= cynk_init,
	.destroy	= cynk_destroy,
	.getattr	= cynk_getattr,
	.fgetattr	= cynk_fgetattr,
	.access		= cynk_access,
	.readlink	= cynk_readlink,
	.opendir	= cynk_opendir,
	.readdir	= cynk_readdir,
	.releasedir	= cynk_releasedir,
	.mknod		= cynk_mknod,
	.mkdir		= cynk_mkdir,
	.symlink	= cynk_symlink,
	.unlink		= cynk_unlink,
	.rmdir		= cynk_rmdir,
	.rename		= cynk_rename,
	.link		= cynk_link,
	.chmod		= cynk_chmod,
	.chown		= cynk_chown,
	.truncate	= cynk_truncate,
	.ftruncate	= cynk_ftruncate,
	.utimens	= cynk_utimens,
	.create		= cynk_create,
	.open		= cynk_open,
	.read		= cynk_read,
	.write		= cynk_write,
	.statfs		= cynk_statfs,
	.flush		= cynk_flush,
	.release	= cynk_release,
	.fsync		= cynk_fsync,
#ifdef HAVE_SETXATTR
	.setxattr	= cynk_setxattr,
	.getxattr	= cynk_getxattr,
	.listxattr	= cynk_listxattr,
	.removexattr= cynk_removexattr,
#endif
	.lock		= cynk_lock,

	.flag_nullpath_ok = 1,
};

static int cynk_fuse_main(struct fuse_args *args)
{
#if FUSE_VERSION >= 26
	return fuse_main(args->argc, args->argv, &cynk_oper, NULL);
#else
	return fuse_main(args->argc, args->argv, &cynk_oper);
#endif
}

static void usage(const char *progname, int full)
{
	printf(
"usage: %s repository [[user@]host:]dir [options]\n"
"\n"
"cynk options:\n"
"    -S                     force a sync\n"
"    -m                     manual sync mode\n"
"    -t N                   duration for next check (60s)\n"
"    -w N                   window time to rest for filesystem activity (30s)\n"
"    -B                     block file system when sync in progress\n"
"    -x directory           online directory accessiable via SSH\n"
"    -z                     disable rsync access\n"
"    -d                     debug cynk only\n"
"    -D                     debug cynk and FUSE\n"
"    -n   --dry-run         dry run\n"
"    -h                     print brief help\n"
"    -H   --help            print full help\n"
"    -V   --version         print version\n"
"    -o opt,[opt...]        SSHFS/FUSE mount options\n"
"\n", progname);

	if (full) {
		printf(
"SSHFS options:\n"
"    -p PORT                equivalent to '-o port=PORT'\n"
"    -C                     equivalent to '-o compression=yes'\n"
"    -F ssh_configfile      specifies alternative ssh configuration file\n"
"    -1                     equivalent to '-o ssh_protocol=1'\n"
"    -o reconnect           reconnect to server\n"
"    -o sshfs_sync          synchronous writes\n"
"    -o no_readahead        synchronous reads (no speculative readahead)\n"
"    -o cache=YESNO         enable caching {yes,no} (default: yes)\n"
"    -o cache_timeout=N     sets timeout for caches in seconds (default: 20)\n"
"    -o cache_X_timeout=N   sets timeout for {stat,dir,link} cache\n"
"    -o workaround=LIST     colon separated list of workarounds\n"
"             none             no workarounds enabled\n"
"             all              all workarounds enabled\n"
"             [no]rename       fix renaming to existing file (default: off)\n"
#ifdef SSH_NODELAY_WORKAROUND
"             [no]nodelay      set nodelay tcp flag in ssh (default: on)\n"
#endif
"             [no]nodelaysrv   set nodelay tcp flag in sshd (default: off)\n"
"             [no]truncate     fix truncate for old servers (default: off)\n"
"             [no]buflimit     fix buffer fillup bug in server (default: on)\n"
"    -o idmap=TYPE          user/group ID mapping, possible types are:\n"
"             none             no translation of the ID space (default)\n"
"             user             only translate UID of connecting user\n"
"    -o ssh_command=CMD     execute CMD instead of 'ssh'\n"
"    -o ssh_protocol=N      ssh protocol to use (default: 2)\n"
"    -o sftp_server=SERV    path to sftp server or subsystem (default: sftp)\n"
"    -o directport=PORT     directly connect to PORT bypassing ssh\n"
"    -o transform_symlinks  transform absolute symlinks to relative\n"
"    -o follow_symlinks     follow symlinks on the server\n"
"    -o no_check_root       don't check for existence of 'dir' on server\n"
"    -o password_stdin      read password from stdin (only for pam_mount!)\n"
"    -o SSHOPT=VAL          ssh options (see man ssh_config)\n"
"\n");
	}
}

static char * chk_norm_dir(const char *path)
{
	struct stat st;
	char *cp, *s;
	
	/* check if a valid path */
	if (stat(path, &st)) {
		error3("Invalid directory %s, not empty or mounted", path);
		return NULL;
	}
	if (!S_ISDIR(st.st_mode)) {
		error3("%s is not a directory", path);
		return NULL;
	}
	
	s = realpath(path, NULL);
	cp = g_strdup(s);
	free(s);
	return cp;
}

static int cynk_opt_proc(void *data, const char *arg, int key,
                          struct fuse_args *outargs)
{
	char *tmp;
	(void) data;

	switch (key) {
	case FUSE_OPT_KEY_OPT:
		if (is_ssh_opt(arg)) {
			tmp = g_strdup_printf("-o%s", arg);
			ssh_add_arg(tmp);
			g_free(tmp);
			return 0;
		}
		return 1;

	case FUSE_OPT_KEY_NONOPT:
		if (!cynk.local) {
			cynk.local = chk_norm_dir(arg);
			if (!cynk.local)
				exit(1);
		} else if (!cynk.remote) {
			if (strchr(arg, ':')) { /* host */
				cynk.remote = g_strdup(arg);
			} else {
				cynk.remote = chk_norm_dir(arg);
				if (!cynk.remote)
					exit(1);
			}
		}
		return 0;
	
	case KEY_PORT:
		tmp = g_strdup_printf("-oPort=%s", arg + 2);
		ssh_add_arg(tmp);
		g_free(tmp);
		return 0;

	case KEY_COMPRESS:
		ssh_add_arg("-oCompression=yes");
		return 0;
	
	case KEY_CONFIGFILE:
		tmp = g_strdup_printf("-F%s", arg + 2);
		ssh_add_arg(tmp);
		g_free(tmp);
		return 0;


	case KEY_HELP:
		usage(outargs->argv[0], 0);
		exit(0);

	case KEY_HELP_FULL:
		usage(outargs->argv[0], 1);
		fuse_opt_add_arg(outargs, "-ho");
		cynk_fuse_main(outargs);		
		exit(0);

	case KEY_VERSION:
		printf("Cynk version %s\n", PACKAGE_VERSION);
#if FUSE_VERSION >= 25
		fuse_opt_add_arg(outargs, "--version");
		cynk_fuse_main(outargs);
#endif
		exit(0);

	case KEY_FOREGROUND:
		cynk.foreground = 1;
		return 1;

	case KEY_DEBUG:
		cynk.debug = 1;
		return 0;
	
	case KEY_DEBUG_ALL:
		cynk.debug = 2;
		return 0;
	
	case KEY_DRYRUN:
		cynk.dryrun = 1;
		return 0;

	case KEY_CMD_SYNC:
		cynk.cmd = CMD_SYNC;
		return 0;

	case KEY_MODE_MANUAL:
		cynk.mode = MODE_MANUAL;
		return 0;

	case KEY_DURATION:
		cynk.duration = atoi(arg + 2);
		return 0;

	case KEY_WINDOW:
		cynk.window = atoi(arg + 2);
		return 0;
	
	case KEY_BLOCK_SYSC:
		cynk.block = 1;
		return 0;

	case KEY_ONLINE_DIR:
		cynk.online_dir = g_strdup(arg + 2);
		if (strcmp(cynk.online_dir, "/") == 0) {
			cynk.len_online_dir = 0;
		} else if (g_str_has_prefix(cynk.online_dir, "/")) {
			cynk.len_online_dir = strlen(cynk.online_dir);
		} else {
			char *tmp = g_strdup_printf("/%s", cynk.online_dir);
			g_free(cynk.online_dir);
			cynk.online_dir = tmp;
			cynk.len_online_dir = strlen(cynk.online_dir);
		}
		cynk.sshfs_on = 1;
		return 0;

	case KEY_DISABLE_RSYNC:
		cynk.rsync_on = 0;
		return 0;

	default:
		error3("internal error");
		abort();
	}
}

#if FUSE_VERSION == 25
static int fuse_opt_insert_arg(struct fuse_args *args, int pos,
                               const char *arg)
{
	assert(pos <= args->argc);
	if (fuse_opt_add_arg(args, arg) == -1)
		return -1;

	if (pos != args->argc - 1) {
		char *newarg = args->argv[args->argc - 1];
		memmove(&args->argv[pos + 1], &args->argv[pos],
			sizeof(char *) * (args->argc - pos - 1));
		args->argv[pos] = newarg;
	}
	return 0;
}
#endif

int main(int argc, char *argv[])
{
	int res;
	char *tmp;														
	char *fsname;		
	int libver;
	struct passwd *pwd;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	memset(&cynk, 0, sizeof(cynk));
	
	g_thread_init(NULL);						
	cynk.blksize = 4096;						
	cynk.max_read = 65536;						
	cynk.max_write = 65536;						
	cynk.nodelay_workaround = 1;					
	cynk.nodelaysrv_workaround = 0;					
	cynk.rename_workaround = 0;					
	cynk.truncate_workaround = 0;					
	cynk.buflimit_workaround = 1;					
	cynk.ssh_ver = 2;						
	cynk.fd = -1;							
	cynk.ptyfd = -1;						
	cynk.ptyslavefd = -1;						
	ssh_add_arg("ssh");						
	ssh_add_arg("-x");						
	ssh_add_arg("-a");						
	ssh_add_arg("-oClearAllForwardings=yes");
	cynk.progname = argv[0];
	cynk.rsync_on = 1;
	cynk.duration = 60;
	cynk.window = 30;
	cynk.online_dir = NULL;
	cynk.mode = MODE_AUTO;
	cynk.uid = getuid();
	cynk.pid = getpid();
	
	pwd = getpwuid(cynk.uid);
	if (!pwd)
		fatal(1, "failed to get pwd for uid %d\n", cynk.uid);
	cynk.gid = pwd->pw_gid;
	cynk.username = g_strdup(pwd->pw_name);
	cynk.userhome = g_strdup(pwd->pw_dir);
	cynk.default_session = g_strdup_printf("%s/cynk-%s/default", SESSION_HOME,
		cynk.username);	
	
	if (fuse_opt_parse(&args, &cynk, cynk_opts, cynk_opt_proc) == -1 ||
	    parse_workarounds() == -1)
		exit(1);
	
	if (cynk.debug >= 1) {
		cynk.foreground = 1;
		fuse_opt_insert_arg(&args, 1, "-f");
		if (!cynk.verbosity)
			cynk.verbosity = 3;
		if (cynk.debug >= 2)
			fuse_opt_insert_arg(&args, 1, "-d");
	}

	if (cynk.cmd) {
		fuse_opt_free_args(&args);
		return cynk_command();
	}
	
	if (!cynk.local || !cynk.remote) {
		fprintf(stderr, 
			"%s: missing repository\n"
			"see `%s -h' for usage\n", 
			cynk.progname, cynk.progname);
		fuse_opt_free_args(&args);
		exit(1);
	}

	cynk.mountpoint = cynk.local;

	/* RSYNC */
	if (cynk.rsync_on) {
		/* create a fake mountpoint to shadow local repository */
		char *dirname = g_path_get_dirname(cynk.mountpoint);
		char *basename = g_path_get_basename(cynk.mountpoint);
		char *tmpstr = rand_str(TEMP_STR_LEN);

		cynk.local = g_strdup_printf("%s/.cynk-%s-%s", dirname, basename, 
			tmpstr);
		cynk.session = g_strdup_printf("%s/cynk-%s/%s", SESSION_HOME,
			cynk.username, tmpstr);
		cynk.cmd_sockpath = g_strdup_printf("%s/sock", cynk.session);
		
		if (cynk.verbosity >= 1)
			verbose(TRUE, "INIT");
		
		if (cynk.verbosity >= 2)
			verbose(FALSE, "* Shadow repository ...");
		if (cynk.verbosity >= 3)
			verbose(FALSE, "> mv %s %s", cynk.mountpoint, cynk.local);
		if (rename(cynk.mountpoint, cynk.local) == -1)
			fatal(1, "failed to move %s to %s", cynk.mountpoint, cynk.local);
		if (cynk.verbosity >= 3)
			verbose(FALSE, "> mkdir %s", cynk.mountpoint);
		if (mkdir(cynk.mountpoint, S_IRWXU) == -1 && errno != EEXIST)
			fatal(1, "failed to create shadow repository %s", cynk.mountpoint);
		
		g_free(dirname);
		g_free(basename);
		g_free(tmpstr);
	}

	/* SSHFS */
	if (cynk.sshfs_on) {
		char *base_path;						
		const char *sftp_server;	
		
		if (cynk.password_stdin) {					
			res = read_password();					
			if (res == -1)						
				exit(1);					
		}								
		
		if (cynk.buflimit_workaround)
			/* Work around buggy sftp-server in OpenSSH.  Without this on	
			   a slow server a 10Mbyte buffer would fill up and the server	
			   would abort */					
			cynk.max_outstanding_len = 8388608;			
		else								
			cynk.max_outstanding_len = ~0;				
		
		if (strchr(cynk.remote, ':')) /* remote host */
			cynk.host = g_strdup(cynk.remote);
		else /* localhost */
			cynk.host = g_strdup_printf("localhost:%s", cynk.remote);

		base_path = find_base_path();					
		if (base_path[0] && base_path[strlen(base_path)-1] != '/')	
			cynk.base_path = g_strdup_printf("%s/", base_path);	
		else								
			cynk.base_path = g_strdup(base_path);			
										
		if (cynk.ssh_command)						
			set_ssh_command();					
										
		tmp = g_strdup_printf("-%i", cynk.ssh_ver);			
		ssh_add_arg(tmp);						
		g_free(tmp);							
		ssh_add_arg(cynk.host);						
		if (cynk.sftp_server)						
			sftp_server = cynk.sftp_server;				
		else if (cynk.ssh_ver == 1)					
			sftp_server = SFTP_SERVER_PATH;				
		else								
			sftp_server = "sftp";					
										
		if (cynk.ssh_ver != 1 && strchr(sftp_server, '/') == NULL)	
			ssh_add_arg("-s");					
										
		ssh_add_arg(sftp_server);
		free(cynk.sftp_server);

		res = processing_init();
		if (res == -1)
			exit(1);

		if (connect_remote() == -1)
			exit(1);

#ifndef SSHFS_USE_INIT
		if (cynk.detect_uid)
			sftp_detect_uid();
#endif

		if (!cynk.no_check_root && sftp_check_root(base_path) == -1)
			exit(1);

		res = cache_parse_options(&args);
		if (res == -1)
			exit(1);

		cynk.randseed = time(0);

		if (cynk.max_read > 65536)
			cynk.max_read = 65536;
		if (cynk.max_write > 65536)
			cynk.max_write = 65536;

		if (fuse_is_lib_option("ac_attr_timeout="))
			fuse_opt_insert_arg(&args, 1, "-oauto_cache,ac_attr_timeout=0");
		tmp = g_strdup_printf("-omax_read=%u", cynk.max_read);
		fuse_opt_insert_arg(&args, 1, tmp);
		tmp = g_strdup_printf("-omax_write=%u", cynk.max_write);
		fuse_opt_insert_arg(&args, 1, tmp);
		g_free(tmp);
		check_large_read(&args);	
	}
	
	fsname = g_strdup(cynk.remote);					
#if FUSE_VERSION >= 27
	libver = fuse_version();
	assert(libver >= 27);
	if (libver >= 28)
		fsname = fsname_escape_commas(fsname);
	else
		fsname_remove_commas(fsname);
	tmp = g_strdup_printf("-osubtype=cynk,fsname=%s", fsname);
#else
	fsname_remove_commas(fsname);
	tmp = g_strdup_printf("-ofsname=cynk#%s", fsname);
#endif
	fuse_opt_insert_arg(&args, 1, tmp);
	g_free(tmp);
	g_free(fsname);
	
	/* insert mountpoint back */
	fuse_opt_insert_arg(&args, 1, cynk.mountpoint);

	umask(0);

	res = cynk_fuse_main(&args);
	
	fuse_opt_free_args(&args);
	if (cynk.sshfs_on) {
		fuse_opt_free_args(&cynk.ssh_args);
		free(cynk.directport);
	}

	return res;
}

/* EOF */
