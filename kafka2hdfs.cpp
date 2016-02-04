/*
g++ -g -W -Wall -I/data/users/data-infra/kafkaclient/cpp -I/usr/local/include/librdkafka -L/data/users/data-infra/kafkaclient/cpp -L/usr/lib/hadoop/lib/native -L/usr/jdk64/jdk1.7.0_45/jre/lib/amd64/server -L/usr/lib64 -o kafka2hdfs kafka2hdfs.cpp -lpykafkaclient -lhdfs -lhadoop -ljvm -lpthread
*/
#include "PyKafkaClient.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "hdfs.h"
#ifdef __cplusplus
}
#endif
#include <stdio.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/select.h>
#include <syslog.h>
#include <dirent.h>
#include <stddef.h>
#include <limits.h>
#include <time.h>
#include <ctype.h>
#include <sys/wait.h>
#include <signal.h>
#include <string>
#include <map>
#include <vector>
#include <deque>
#include <sstream>

#define CWD_ROOT "/data/users/data-infra/kafka2hdfs/"

class ThrArg {
public:
    kafka_client_topic_t *kct;
    int partno;
    ThrArg(): kct(NULL), partno(0)
    {
    }
    ThrArg(kafka_client_topic_t *kct, int partno): kct(kct), partno(partno)
    {
    }
    ~ThrArg()
    {
    }
};

static char *consumer_conf_path;
static std::map<std::string, std::string> k2h_conf;

static int topic_total;
static char **topics = NULL;
static int *partnos = NULL;

static std::map<std::string, int> upload_intervals;

static kafka_consumer_t *consumer = NULL;
static kafka_client_topic_t **kcts = NULL;

static const char *hdfs_path_temp = "/user/data-infra";
static const char *hdfs_path_express = "/user/data-infra/express";
static const char *hdfs_path_expressBid = "/user/data-infra/express_bid";
static const char *hdfs_path_selfmedia = "/user/data-infra/selfmedia";
static const char *hdfs_path_logAccess = "/user/data-infra/log_access";
static const char *hdfs_path_3rdStats = "/user/data-infra/3th_stats";
static const char *hdfs_path_rc = "/user/data-infra/rc";
static std::map<const char *, const char *> topic_hdfs_map;

static std::map<std::string, FILE *> fp_cache;
static pthread_rwlock_t fp_lock = PTHREAD_RWLOCK_INITIALIZER;

static char *app_log_path = NULL;

static int64_t start_offset = KAFKA_OFFSET_STORED;

static std::map<std::string, std::deque<std::string> > upload_queues;
static std::map<std::string, pthread_mutex_t> upload_mutexes;

// TODO optimaztion needed
static bool align_YmdHM(char *YmdHM, int interval)
{
    if (strlen(YmdHM) != 12) return false;

    struct tm r;
    char c;

    r.tm_sec = 0;
    r.tm_min = atoi(YmdHM + 10);

    c = YmdHM[10];
    YmdHM[10] = '\0';
    r.tm_hour = atoi(YmdHM + 8);
    YmdHM[10] = c;

    c = YmdHM[8];
    YmdHM[8] = '\0';
    r.tm_mday = atoi(YmdHM + 6);
    YmdHM[8] = c;

    c = YmdHM[6];
    YmdHM[6] = '\0';
    r.tm_mon = atoi(YmdHM + 4) - 1;
    YmdHM[6] = c;

    c = YmdHM[4];
    YmdHM[4] = '\0';
    r.tm_year = atoi(YmdHM) - 1900;
    YmdHM[4] = c;

    r.tm_isdst = 0;

    time_t t = mktime(&r);
    if (t == -1) return false;

    long rem = t % (interval * 60);
    if (rem == 0) return true;

    t += interval * 60 - rem;
    if (localtime_r(&t, &r) == NULL) return false;
    snprintf(YmdHM, 13, "%d%02d%02d%02d%02d",
             r.tm_year + 1900, r.tm_mon + 1, r.tm_mday, r.tm_hour, r.tm_min);

    return true;
}

static inline void free_topics_partnos()
{
    if (topics != NULL) {
        for (int i = 0; i < topic_total; i++) {
            if (topics[i] != NULL) {
                free(topics[i]);
                topics[i] = NULL;
            }
        }
        free(topics);
        topics = NULL;
    }
    if (partnos != NULL) {
        free(partnos);
        partnos = NULL;
    }
}

static inline void free_consumer()
{
    if (consumer != NULL) {
        destroy_kafka_consumer(consumer);
        consumer = NULL;
    }
}

static bool parse_conf_file(const char *conf_path)
{
    FILE *fp = fopen(conf_path, "r");
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR, "fopen[%s] failed with errno[%d]",
                  conf_path, errno);
        return false;
    }

    char line[512], *p1, *p2, *p3;

    k2h_conf.clear();
    while (fgets(line, sizeof(line), fp) != NULL) {
        if ((p1 = strchr(line, '=')) == NULL) {
            write_log(app_log_path, LOG_WARNING, "invalid conf line[%s]", line);
            continue;
        }

        p2 = line;
        while (isspace(*p2) && p2 < p1) p2++;
        if (*p2 == '#') continue;
        if (p2 == p1) {
            write_log(app_log_path, LOG_WARNING, "invalid conf line[%s]", line);
            continue;
        }
        p3 = p1 - 1;
        while (isspace(*p3)) p3--;
        std::string name(p2, p3 - p2 + 1);

        p2 = p1 + 1;
        while (isspace(*p2) && *p2 != '\0') p2++;
        if (*p2 == '\0') {
            write_log(app_log_path, LOG_WARNING, "invalid conf line[%s]", line);
            continue;
        }
        p3 = line + strlen(line) - 1;
        while (isspace(*p3)) p3--;
        std::string value(p2, p3 - p2 + 1);

        k2h_conf.insert(std::pair<std::string, std::string>(name, value));
    }
    if (ferror(fp)) {
        write_log(app_log_path, LOG_ERR, "ferror occurred");
    }

    fclose(fp);
#if 1
for (std::map<std::string, std::string>::iterator it = k2h_conf.begin();
     it != k2h_conf.end();
     it++) {
    fprintf(stderr, "key[%s] value[%s]\n",
            it->first.c_str(), it->second.c_str());
}
#endif
    return true;
}

// simply implemented with low performance
static void str_split(const std::string& s, char delim,
                      std::vector<std::string>& tokens)
{
    std::string t;
    std::stringstream ss(s);

    // XXX
    while (getline(ss, t, delim)) {
        tokens.push_back(t);
    }
}

static int extract_topics_partnos(const std::string& s)
{
    int n = 0, m;
    std::vector<std::string> vec, vec2;

    str_split(s, ',', vec);
    topic_total = vec.size();
    if ((topics = (char **)calloc(topic_total, sizeof(char *))) == NULL) {
        write_log(app_log_path, LOG_ERR, "malloc for topics failed");
        return n;
    }
    if ((partnos = (int *)malloc(topic_total * sizeof(int))) == NULL) {
        write_log(app_log_path, LOG_ERR, "malloc for partnos failed");
        free(topics);
        topics = NULL;
        return n;
    }
    for (int i = 0; i < topic_total; i++) {
        str_split(vec[i], ':', vec2);
        m = atoi(vec2[1].c_str());
        topics[i] = strdup(vec2[0].c_str());
        partnos[i] = m;
        n += m;
        vec2.clear();
    }

    return n;
}

static void extract_intervals(const std::string& s)
{
    std::vector<std::string> vec, vec2;

    str_split(s, ',', vec);
    for (size_t i = 0, l = vec.size(); i < l; i++) {
        str_split(vec[i], ':', vec2);
        upload_intervals[vec2[0]] = atoi(vec2[1].c_str());
        vec2.clear();
    }
}

#define TIME_FIELD_INDEX 6
#define TIME_FIELD_INDEX_PUB 11
#define FIELD_DELIMITER '\t'

static bool extract_YmdHM(const char *topic, const char *s, char *YmdHM)
{
    int n = 0, index;
    const char *temp = s, *p;

    index = strcmp(topic, "pub") == 0 ? TIME_FIELD_INDEX_PUB : TIME_FIELD_INDEX;

    while (temp != NULL && (p = strchr(temp, FIELD_DELIMITER)) != NULL) {
        if (++n == index) {
            // 7th or 11th field is empty
            if (*(p + 1) == FIELD_DELIMITER) return false;
            snprintf(YmdHM, 13, "%s", p + 1);
            return true;
        }
        temp = p + 1;
    }

    return false;
}

#define ACTIONTYPE_FIELD_INDEX 2

static int extract_action_type(const char *s)
{
    int n = 0;
    const char *temp = s, *p;
    char r[4];

    while (temp != NULL && (p = strchr(temp, FIELD_DELIMITER)) != NULL) {
        if (++n == ACTIONTYPE_FIELD_INDEX) {
            // 3rd field is empty
            if (*(p + 1) == FIELD_DELIMITER) return 0;
            snprintf(r, 4, "%s", p + 1);
            return atoi(r);
        }
        temp = p + 1;
    }

    return 0;
}

#define DEVICE_FIELD_INDEX 41

static bool extract_device(const char *s, char *device)
{
    int n = 0;
    const char *temp = s, *p;

    while (temp != NULL && (p = strchr(temp, FIELD_DELIMITER)) != NULL) {
        if (++n == DEVICE_FIELD_INDEX) {
            if (*(p + 1) == FIELD_DELIMITER) {
                snprintf(device, 8, "pc"); // the 42th field is empty
            } else {
                snprintf(device, 8, "%s", p + 1);
                if (strncmp(device, "General", 7) == 0
                    || strncmp(device, "Na", 2) == 0) {
                    snprintf(device, 8, "pc");
                } else {
                    snprintf(device, 8, "mobile");
                }
            }
            return true;
        }
        temp = p + 1;
    }

    return false;
}

static void destroy_topics()
{
    if (kcts != NULL) {
        for (int i = 0; i < topic_total; i++) {
            if (kcts[i] != NULL) {
                del_topic(kcts[i]);
                kcts[i] = NULL;
            }
        }
        kcts = NULL;
    }
}

static bool init_topics()
{
    if ((kcts = (kafka_client_topic_t **)calloc(topic_total,
                                               sizeof(kafka_client_topic_t *)))
        == NULL) {
        write_log(app_log_path, LOG_ERR, "callloc for kcsts failed");
        return false;
    }
    for (int i = 0; i < topic_total; i++) {
        kafka_client_topic_t *kct = set_consumer_topic(consumer, topics[i]);
        if (kct == NULL) {
            destroy_topics();
            return false;
        }
        kcts[i] = kct;
    }
    return true;
}

static FILE *retrieve_fp(const char *filename)
{
    std::string bn;
    try {
        bn.assign(basename(filename));
    } catch (std::exception& e) {
        write_log(app_log_path, LOG_WARNING,
                  "string assign failed with exception[%s]", e.what());
        return NULL;
    } catch (...) {
        write_log(app_log_path, LOG_WARNING,
                  "string assign failed with unknown exception");
        return NULL;
    }

    FILE *fp;

    pthread_rwlock_rdlock(&fp_lock);
    std::map<std::string, FILE *>::const_iterator it = fp_cache.find(bn);
    if (it == fp_cache.end()) {
        pthread_rwlock_unlock(&fp_lock);
        pthread_rwlock_wrlock(&fp_lock);
        if ((it = fp_cache.find(bn)) != fp_cache.end()) {
            fp = it->second;
        } else {
            if ((fp = fopen(filename, "a")) == NULL) {
                write_log(app_log_path, LOG_ERR, "fopen[%s] for writing raw logs"
                          " failed with errno[%d]", filename, errno);
            } else {
                std::pair<std::map<std::string, FILE *>::iterator, bool> ret;
                ret = fp_cache.insert(std::pair<std::string, FILE *>(bn, fp));
                if (!ret.second) {
                    write_log(app_log_path, LOG_WARNING,
                              "repeated FP[%s]", filename);
                    fclose(fp);
                    fp = ret.first->second;
                }
            }
        }
    } else {
        fp = it->second;
    }
    pthread_rwlock_unlock(&fp_lock);

    return fp;
}

static void cleanup_fp(const char *filename)
{
    std::string bn;
    try {
        bn.assign(basename(filename));
    } catch (std::exception& e) {
        write_log(app_log_path, LOG_WARNING,
                  "string assign failed with exception[%s]", e.what());
        return;
    } catch (...) {
        write_log(app_log_path, LOG_WARNING,
                  "string assign failed with unknown exception");
        return;
    }

    pthread_rwlock_wrlock(&fp_lock);
    std::map<std::string, FILE *>::iterator it = fp_cache.find(bn);
    if (it == fp_cache.end()) {
        write_log(app_log_path, LOG_WARNING,
                  "invalid key[%s] for fp_cache", bn.c_str());
    } else {
        fclose(it->second);
        fp_cache.erase(it);
    }
    pthread_rwlock_unlock(&fp_lock);
}

static void clear_fp_cache()
{
    pthread_rwlock_wrlock(&fp_lock);
    for (std::map<std::string, FILE *>::iterator it = fp_cache.begin();
         it != fp_cache.end(); it++) {
        fclose(it->second);
    }
    fp_cache.clear();
    pthread_rwlock_unlock(&fp_lock);
}

static bool is_digit_str(const char *s, size_t l)
{
    for (size_t i = 0; i < l; i++) {
        if (!isdigit(s[i])) return false;
    }
    return true;
}

#define RAW_LOG_PATH "./k2h_log"
#define UPLOAD_PATH "./upload"

static bool build_local_path(const char *payload, const char *topic, long ymdhm,
                             char *path)
{
    if (strcmp(topic, "unbid") == 0 || strcmp(topic, "bid") == 0) {
        char device[8];
        if (!extract_device(payload, device)) {
            write_log(app_log_path, LOG_ERR,
                      "extract_device[%s] topic[%s] failed", payload, topic);
            return false;
        }
        snprintf(path, 128, "%s%s/%s/%s_%ld_%s.seq",
                 CWD_ROOT, RAW_LOG_PATH, topic, topic, ymdhm, device);
    } else if (strcmp(topic, "imp") == 0 || strcmp(topic, "ic") == 0) {
        int actiontype = extract_action_type(payload);
        switch (actiontype) {
        case 1: // impression
            snprintf(path, 128, "%s%s/%s/imp_%ld.seq",
                     CWD_ROOT, RAW_LOG_PATH, topic, ymdhm);
            break;
        case 2: // click
            snprintf(path, 128, "%s%s/%s/click_%ld.seq",
                     CWD_ROOT, RAW_LOG_PATH, topic, ymdhm);
            break;
        default:
            write_log(app_log_path, LOG_WARNING,
                      "invalid action type[%d] for topic[imp|ic] msg[%s]",
                      actiontype, payload);
            return false;
        }
    } else if (strcmp(topic, "stats") == 0) {
        int actiontype = extract_action_type(payload);
        switch (actiontype) {
        case 11: // impression
            snprintf(path, 128, "%s%s/%s/imp_%ld.seq",
                     CWD_ROOT, RAW_LOG_PATH, topic, ymdhm);
            break;
        case 12: // click
            snprintf(path, 128, "%s%s/%s/click_%ld.seq",
                     CWD_ROOT, RAW_LOG_PATH, topic, ymdhm);
            break;
        default: // invalid for ic or stats
            write_log(app_log_path, LOG_WARNING,
                      "invalid action type[%d] for topic[stats]msg[%s]",
                      actiontype, payload);
            return false;
        }
    } else {
        snprintf(path, 128, "%s%s/%s/%s_%ld.seq",
                 CWD_ROOT, RAW_LOG_PATH, topic, topic, ymdhm);
    }

    return true;
}

static void delay_simply(int milli)
{
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = milli * 1000;
    if (select(0, NULL, NULL, NULL, &tv) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "select failed with errno[%d]", errno);
    }
}

static bool write_raw_file(const char *topic, const char *payload, size_t len)
{
    std::map<std::string, int>::const_iterator it =
        upload_intervals.find(topic);
    if (it == upload_intervals.end()) {
        write_log(app_log_path, LOG_ERR,
                  "misconfigured topic[%s] for interval", topic);
        return false;
    }

    // request time
    char YmdHM[13];
    if (!extract_YmdHM(topic, payload, YmdHM)) {
        write_log(app_log_path, LOG_WARNING,
                  "extract_YmdHM[%s] topic[%s] failed", payload, topic);
        return false;
    }
    if (is_digit_str(YmdHM, 12)) {
        if (!align_YmdHM(YmdHM, it->second)) {
            write_log(app_log_path, LOG_ERR,
                      "align_YmdHM[%s] failed for interval[%d]",
                      YmdHM, it->second);
            return false;
        }
    }

    long ymdhm = atol(YmdHM);
    if (ymdhm < 201500000000) {
        write_log(app_log_path, LOG_WARNING,
                  "topic[%s] YmdHM[%s] interval[%d] ymdhm[%ld]",
                  topic, YmdHM, it->second, ymdhm);
    }

    // local path
    char loc_path[256];
    if (!build_local_path(payload, topic, ymdhm, loc_path)) return false;

    FILE *fp = retrieve_fp(loc_path);
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR, "retrieve_fp[%s] failed", loc_path);
        return false;
    }

    int count = 0;
    size_t n = fwrite(payload, 1, len, fp);
    while (n != len) {
        if (++count == 3) break;
        delay_simply(500 * count);
        len -= n;
        payload += n;
        n = fwrite(payload, 1, len, fp);
    }
    if (count == 3) {
        write_log(app_log_path, LOG_WARNING, "fwrite[%s] might fail", loc_path);
        if ((fp = retrieve_fp(loc_path)) == NULL) {
            write_log(app_log_path, LOG_ERR,
                      "retrieve_fp[%s] failed", loc_path);
            return false;
        }
        if ((n = fwrite(payload, 1, len, fp)) != len) {
            write_log(app_log_path, LOG_ERR, "fwrite[%s] failed", loc_path);
            return false;
        }
    }
    fputc('\n', fp);

    return true;
}

static void consume(kafka_message_t *message, void *opaque)
{
    const char *topic = get_topic_by_message(message);
    if (topic == NULL) {
        write_log(app_log_path, LOG_ERR, "get_topic_by_message failed");
        return;
    }
    write_raw_file(topic, (char *)message->payload, message->len);
}

static void *consume_to_local(void *arg)
{
    write_log(app_log_path, LOG_INFO, "thread consume_to_local created");

    ThrArg *a = (ThrArg *)arg;
    int ret = 0;

    int n = consume_messages(consumer, a->kct, a->partno,
                             start_offset, consume);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR,
                  "consume_messages for topic[%s] partition[%d] failed",
                  get_topic(a->kct), a->partno);
        ret = -1;
        goto rtn;
    }

rtn:
    delete a;
    write_log(app_log_path, LOG_ERR, "thread consume_to_local exiting");
    return (void *)(intptr_t)ret;
}

static time_t diff_atime_mtime(const char *path)
{
    struct stat stbuf;
    if (stat(path, &stbuf) != 0) {
        write_log(app_log_path, LOG_ERR, "stat[%s] for elapse "
                  " from atime to mtime failed with errno[%d]", path, errno);
        return -1;
    }
    return stbuf.st_mtime - stbuf.st_atime;
}

static time_t get_mtime(const char *path)
{
    struct stat stbuf;
    if (stat(path, &stbuf) != 0) {
        write_log(app_log_path, LOG_ERR,
                  "stat[%s] for mtime failed with errno[%d]", path, errno);
        return -1;
    }
    return stbuf.st_mtime;
}

static time_t get_file_size(const char *path)
{
    struct stat stbuf;
    if (stat(path, &stbuf) != 0) {
        write_log(app_log_path, LOG_ERR,
                  "stat[%s] for size failed with errno[%d]", path, errno);
        return -1;
    }
    return stbuf.st_size;
}

#define BUFFER_MINUTES 5

//static bool is_write_finished(const char *path, const char *topic)
static bool is_write_finished(const char *path)
{
    time_t mt = get_mtime(path);
    if (mt == -1) return false;

#if 0
    std::map<std::string, int>::const_iterator it =
        upload_intervals.find(topic);
    if (it == upload_intervals.end()) {
        write_log(app_log_path, LOG_ERR,
                  "misconfigured interval for topic[%s]", topic);
        return false;
    }
#endif

    // XXX how long to delay?
    // front-end machines --> Kafka --> consumer
    return (time(NULL) - mt) > BUFFER_MINUTES * 60;
}

static inline void rm_local(const char *f, int flag = 1)
{
    switch (flag) {
    case 1: // delete
        write_log(app_log_path, LOG_INFO, "DEBUG unlinking[%s]", f);
        if (unlink(f) != 0) {
            write_log(app_log_path, LOG_WARNING,
                      "unlink[%s] failed with errno[%d]", f, errno);
        }
        break;
    case 2: // move
        break;
    }
}

static int wrap_system(const char *cmd, bool logged = true)
{
    errno = 0;

    int ret = system(cmd);
    if (ret == -1) {
        if (logged)
            write_log(app_log_path, LOG_ERR,
                      "system[%s] failed with errno[%d]", cmd, errno);
    } else {
        if (WIFEXITED(ret)) {
            if ((ret = WEXITSTATUS(ret)) != 0) {
                if (logged)
                    write_log(app_log_path, LOG_ERR,
                              "cmd[%s] exited with errcode[%d]", cmd, ret);
                ret = -1;
            }
        } else if (WIFSIGNALED(ret)) {
            if (logged)
                write_log(app_log_path, LOG_ERR,
                          "cmd[%s] exited with signal[%d]", cmd, WTERMSIG(ret));
            ret = -1;
        } else if (WIFSTOPPED(ret)) {
            if (logged)
                write_log(app_log_path, LOG_ERR,
                          "cmd[%s] stopped with signal[%d]",
                          cmd, WSTOPSIG(ret));
            ret = -1;
        } else {
            if (logged)
                write_log(app_log_path, LOG_ERR,
                          "cmd[%s] exited abnormally", cmd);
            ret = -1;
        }
    }

    return ret;
}

//#define LZOP "/usr/local/bin/lzop -1 -U -f -S .inpro --ignore-warn"
#define LZOP "/usr/local/bin/lzop -1 -U -f --ignore-warn"

// TODO should be implemented using lzo lib directly
static int lzo_compress(const char *in_path)
{
    char cmd[256];
    //snprintf(cmd, sizeof(cmd), "%s %s &", LZOP, in_path);
    snprintf(cmd, sizeof(cmd), "%s %s &", LZOP, in_path);
    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define MAX_FILE_SIZE 8589934592 // 8G

// only bid and unbid need to be compressed
static void *compress_files(void *arg)
{
    char *topic = (char *)arg;
    write_log(app_log_path, LOG_INFO, "thread compress_files"
              " for topic[%s] created", topic);

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_detach[compress_files]"
                  " failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread compress_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    if (strcmp(topic, "bid") != 0 && strcmp(topic, "unbid") != 0) {
        write_log(app_log_path, LOG_WARNING,
                  "topic[%s] need not to be compressed", topic);
        write_log(app_log_path, LOG_ERR, "thread compress_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    char path[sizeof(RAW_LOG_PATH) + 128], path2[sizeof(RAW_LOG_PATH) + 128];
    int n = snprintf(path, sizeof(path), "%s%s/%s",
                     CWD_ROOT, RAW_LOG_PATH, topic);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf topic[%s] failed", topic);
        write_log(app_log_path, LOG_ERR, "thread compress_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    DIR *dp = opendir(path);
    if (dp == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "opendir[%s] failed with errno[%d]", path, errno);
        write_log(app_log_path, LOG_ERR, "thread compress_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    size_t el = offsetof(struct dirent, d_name) + 64;
    struct dirent *dep = (struct dirent *)malloc(el);
    if (dep == NULL) {
        write_log(app_log_path, LOG_ERR, "malloc failed");
        closedir(dp);
        write_log(app_log_path, LOG_ERR, "thread compress_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    memcpy(path2, path, n);

    struct dirent *res;
    while (readdir_r(dp, dep, &res) == 0) {
        if (res == NULL) {
            sleep(70);
            rewinddir(dp);
            continue;
        }
        if (strcmp(dep->d_name, ".") == 0 || strcmp(dep->d_name, "..") == 0) {
            continue;
        }
        char *p = strrchr(dep->d_name, '.') + 1;
        if (strncmp(p, "seq", 3) == 0) {
            snprintf(path + n, sizeof(path) - n, "/%s", dep->d_name);
            if (is_write_finished(path)) {
                snprintf(path2 + n, sizeof(path2) - n, "/%s.0", dep->d_name);
                write_log(app_log_path, LOG_INFO,
                          "DEBUG path[%s] writing finished", path);
            } else if (get_file_size(path) > MAX_FILE_SIZE) {
                snprintf(path2 + n, sizeof(path2) - n, "/%s.%ld",
                         dep->d_name, time(NULL));
                write_log(app_log_path, LOG_INFO, "DEBUG path[%s] too large", path2);
            } else {
                continue;
            }
            // XXX
            if (rename(path, path2) != 0) {
                write_log(app_log_path, LOG_ERR, "rename[%s to %s] failed"
                          " with errno[%d]", path, path2, errno);
                continue;
            }
            cleanup_fp(path);
            // TODO non-blocking exit code
            lzo_compress(path2);
        }
    }
    write_log(app_log_path, LOG_ERR,
              "readdir_r for topic[%s] to compress failed", topic);

    free(dep);
    closedir(dp);
    write_log(app_log_path, LOG_ERR, "thread compress_files"
              " for topic[%s] exiting", topic);

    return (void *)0;
}

static void init_topic_hdfs_map()
{
    for (int i = 0; i < topic_total; i++) {
        char *topic = topics[i];
        if (strcmp(topic, "bid") == 0) {
            topic_hdfs_map[topic] = hdfs_path_expressBid;
        } else if (strcmp(topic, "unbid") == 0) {
            topic_hdfs_map[topic] = hdfs_path_expressBid;
        } else if (strcmp(topic, "pdb-bid") == 0) {
            topic_hdfs_map[topic] = hdfs_path_selfmedia;
        } else if (strcmp(topic, "pdb-unbid") == 0) {
            topic_hdfs_map[topic] = hdfs_path_selfmedia;
        } else if (strcmp(topic, "imp") == 0) {
            topic_hdfs_map[topic] = hdfs_path_selfmedia;
        } else if (strcmp(topic, "unimp") == 0) {
            topic_hdfs_map[topic] = hdfs_path_selfmedia;
        } else if (strcmp(topic, "adv") == 0) {
            topic_hdfs_map[topic] = hdfs_path_express;
        } else if (strcmp(topic, "cvt") == 0) {
            topic_hdfs_map[topic] = hdfs_path_express;
        } else if (strcmp(topic, "ic") == 0) {
            topic_hdfs_map[topic] = hdfs_path_express;
        } else if (strcmp(topic, "pub") == 0) {
            topic_hdfs_map[topic] = hdfs_path_logAccess;
        } else if (strcmp(topic, "stats") == 0) {
            topic_hdfs_map[topic] = hdfs_path_3rdStats;
        } else if (strcmp(topic, "rc") == 0) {
            topic_hdfs_map[topic] = hdfs_path_rc;
        }
    }
}

static bool build_hdfs_path(const char *topic, const char *bn, char *path)
{
    const char *p = strchr(bn, '_');
    if (p == NULL) {
        write_log(app_log_path, LOG_ERR, "invalid filename[%s]", bn);
        return false;
    }

    const char *q = strchr(p + 1, '_');
    if (q == NULL) {
        q = strchr(p + 1, '.');
        if (q == NULL) {
            write_log(app_log_path, LOG_ERR, "invalid filename[%s]", bn);
            return false;
        }
    }

    if (q - p < 13) {
        write_log(app_log_path, LOG_WARNING, "truncated filename[%s]", bn);
        snprintf(path, 256, "%s/%s", hdfs_path_temp, bn);
        return true;
    }

    int n;
    const char *path_root = topic_hdfs_map[topic];
    if (strcmp(path_root, hdfs_path_selfmedia) == 0) {
        if (strcmp(topic, "imp") == 0) {
            if (strncmp("imp", bn, 3) == 0) {
                n = snprintf(path, 256, "%s/imp/", path_root);
            } else if (strncmp("click", bn, 5) == 0) {
                n = snprintf(path, 256, "%s/click/", path_root);
            }
        } else if (strcmp(topic, "unimp") == 0) {
            n = snprintf(path, 256, "%s/imp/", path_root);
        } else {
            n = snprintf(path, 256, "%s/%s/", path_root, topic);
        }
    } else if (strcmp(path_root, hdfs_path_3rdStats) == 0) {
        if (strncmp("imp", bn, 3) == 0) {
            n = snprintf(path, 256, "%s/impression/", path_root);
        } else if (strncmp("click", bn, 5) == 0) {
            n = snprintf(path, 256, "%s/click/", path_root);
        }
    } else {
        n = snprintf(path, 256, "%s/", path_root);
    }
    snprintf(path + n, 256 - n, "%c%c%c%c/%c%c/%c%c/%c%c/%s",
             *(p + 1), *(p + 2), *(p + 3), *(p + 4), *(p + 5), *(p + 6),
             *(p + 7), *(p + 8), *(p + 9), *(p + 10), bn);

    return true;
}

#define HDFS_PUT "hadoop fs -put"

// TODO for large files to be uploaded
// should be implemented using libhdfs directly
// referring Impala
static int hdfs_put(const char *loc_path, const char *hdfs_path)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "%s %s %s", HDFS_PUT, loc_path, hdfs_path);
    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define HDFS_APPEND "hadoop fs -appendToFile"

static int hdfs_append(const char *loc_path, const char *hdfs_path)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "%s %s %s", HDFS_APPEND, loc_path, hdfs_path);
    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define HADOOP_LZO_INDEX "hadoop jar /usr/lib/hadoop/lib/" \
    "hadoop-lzo-0.4.19.jar com.hadoop.compression.lzo.LzoIndexer"

static int hdfs_lzo_index(const char *hdfs_path)
{
    char cmd[256];
    //snprintf(cmd, sizeof(cmd), "%s %s", HADOOP_LZO_INDEX, hdfs_path);
    snprintf(cmd, sizeof(cmd), "%s %s &", HADOOP_LZO_INDEX, hdfs_path);
    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define HDFS_USER "data-infra"

static int copy(const char *topic, const char *zfile)
{
    char hdfs_path[256];
    if (!build_hdfs_path(topic, basename(zfile), hdfs_path)) {
        return -1;
    }

    hdfsFS fs_handle = hdfsConnectAsUser(k2h_conf["namenode"].c_str(),
                                         atoi(k2h_conf["port"].c_str()),
                                         HDFS_USER);
    if (fs_handle == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "hdfsConnect[%s:%s] failed with errno[%d]",
                  k2h_conf["namenode"].c_str(), k2h_conf["port"].c_str(), errno);
        return -1;
    }

    struct stat st;
    if (stat(zfile, &st) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "stat[%s] failed with errno[%d]", zfile, errno);
        return -1;
    }

    bool existing = hdfsExists(fs_handle, hdfs_path) == 0;

    if (st.st_size > INT_MAX) {
        char *p = strrchr(hdfs_path, '/');
        *p = '\0';
        if (hdfsExists(fs_handle, hdfs_path) != 0) {
            if (hdfsCreateDirectory(fs_handle, hdfs_path) != 0) {
                write_log(app_log_path, LOG_ERR,
                          "hdfsCreateDirectory[%s] failed with errno[%d]",
                          hdfs_path, errno);
                return -1;
            }
        }
        *p = '/';
        if (existing) {
            if (hdfs_append(zfile, hdfs_path) == 0) {
                write_log(app_log_path, LOG_INFO,
                          "DEBUG hdfs_append[%s] OK", zfile);
                return 0;
            } else {
                return -1;
            }
        } else {
            if (hdfs_put(zfile, hdfs_path) == 0) {
                write_log(app_log_path, LOG_INFO,
                          "DEBUG hdfs_put[%s] OK", zfile);
                return 0;
            } else {
                return -1;
            }
        }
    }

    int flags = existing ? (O_WRONLY | O_APPEND) : (O_WRONLY | O_CREAT);
    hdfsFile file_handle = hdfsOpenFile(fs_handle, hdfs_path, flags, 0, 0, 0);
    if (file_handle == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "hdfsOpenFile[%s] failed with errno[%d]", zfile, errno);
        // XXX
        // comments borrowed from Impala:
/// A (process-wide) cache of hdfsFS objects.
/// These connections are shared across all threads and kept open until the process
/// terminates.
//
/// These connections are leaked, i.e. we never call hdfsDisconnect(). Calls to
/// hdfsDisconnect() by individual threads would terminate all other connections handed
/// out via hdfsConnect() to the same URI, and there is no simple, safe way to call
/// hdfsDisconnect() when process terminates (the proper solution is likely to create a
/// signal handler to detect when the process is killed, but we would still leak when
/// impalad crashes).
        //hdfsDisconnect(fs_handle);
        return -1;
    }

    FILE *fp = fopen(zfile, "r");
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR, "fopen[%s] failed with errno[%d]",
                  zfile, errno);
        hdfsCloseFile(fs_handle, file_handle);
        return -1;
    }

    char *buf = (char *)malloc(st.st_size);
    if (buf == NULL) {
        write_log(app_log_path, LOG_ERR, "malloc failed");
        fclose(fp);
        hdfsCloseFile(fs_handle, file_handle);
        return -1;
    }

    int ret = 0;

    clearerr(fp);
    size_t n = fread(buf, 1, st.st_size, fp);
    if (n != (size_t)st.st_size) {
        write_log(app_log_path, LOG_ERR,
                  "fread[%s] failed (read[%lu] expected[%ld])",
                  zfile, n, st.st_size);
        ret = -1;
        goto rtn;
    }
    if ((ret = ferror(fp)) != 0) {
        write_log(app_log_path, LOG_ERR,
                  "fread[%s] failed with ferror returning[%d]",
                  zfile, ret);
        ret = -1;
        goto rtn;
    }

    if (hdfsWrite(fs_handle, file_handle, (void *)buf, n) == -1) {
        write_log(app_log_path, LOG_ERR, "hdfsWrite[%s] failed with errno[%d]",
                  hdfs_path, errno);
        ret = -1;
        goto rtn;
    }

    if (hdfsFlush(fs_handle, file_handle) == -1) {
        // TODO
        write_log(app_log_path, LOG_ERR, "hdfsFlush[%s] failed with errno[%d]",
                  hdfs_path, errno);
        ret = -1;
        goto rtn;
    }

rtn:
    free(buf);
    fclose(fp);
    hdfsCloseFile(fs_handle, file_handle);
    return ret;
}

static int copy_ex(const char *topic, const char *zfile, bool indexed = true)
{
    char hdfs_path[256];
    if (!build_hdfs_path(topic, basename(zfile), hdfs_path)) {
        return -1;
    }

    char *p = strrchr(hdfs_path, '/');
    *p = '\0';

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "hadoop fs -test -e %s", hdfs_path);

    if (wrap_system(cmd, false) != 0) {
        snprintf(cmd, sizeof(cmd), "hadoop fs -mkdir -p %s", hdfs_path);
        if (wrap_system(cmd) != 0) return -1;
    }

    *p = '/';

    snprintf(cmd, sizeof(cmd), "hadoop fs -test -e %s", hdfs_path);
    if (wrap_system(cmd, false) != 0) {
        if (hdfs_put(zfile, hdfs_path) == 0) {
write_log(app_log_path, LOG_INFO, "DEBUG hdfs_put[%s] OK", zfile);
        } else {
            return -1;
        }
    } else {
        if (hdfs_append(zfile, hdfs_path) == 0) {
write_log(app_log_path, LOG_INFO, "DEBUG hdfs_append[%s] OK", zfile);
        } else {
            return -1;
        }
    }
    if (indexed && hdfs_lzo_index(hdfs_path) == 0) {
write_log(app_log_path, LOG_INFO, "DEBUG hdfs_lzo_index[%s] OK", hdfs_path);
    }
    return 0;
}

static int scandir_filter(const struct dirent *dep)
{
    if (strcmp(dep->d_name, ".") == 0 || strcmp(dep->d_name, "..") == 0) {
        return 0;
    }
    if (strncmp(dep->d_name, "unbid", sizeof("unbid") - 1) == 0
        || strncmp(dep->d_name, "bid", sizeof("bid") - 1) == 0) {
        const char *p = strrchr(dep->d_name, '.');
        if (p == NULL) return 0;
        return strncmp(p + 1, "lzo", 3) == 0 ? 1 : 0;
    } else {
        return 1;
    }
}

static int scandir_compar(const struct dirent **a, const struct dirent **b)
{
    if (strncmp((*a)->d_name, "unbid", sizeof("unbid") - 1) == 0
        || strncmp((*a)->d_name, "bid", sizeof("bid") - 1) == 0) {
        const char *p = strchr((*a)->d_name, '_'),
              *q = strchr((*b)->d_name, '_');
        int r = strncmp(p + 1, q + 1, 12);
        if (r == 0) {
            p = strchr(p, '.');
            q = strchr(q, '.');
            return strcmp(p + 1, q + 1);
        } else {
            return r;
        }
    } else {
        return strcmp((*a)->d_name, (*b)->d_name);
    }
}

static void *enqueue_upload_files(void *arg)
{
    char *topic = (char *)arg;
    write_log(app_log_path, LOG_INFO, "thread enqueue_upload_files"
              " for topic[%s] created", topic);

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_detach[enqueue_upload_files]"
                  " failed with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread enqueue_upload_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    char path[sizeof(RAW_LOG_PATH) + 128];
    int n = snprintf(path, sizeof(path), "%s%s/%s",
                     CWD_ROOT, RAW_LOG_PATH, topic);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf failed");
        write_log(app_log_path, LOG_ERR, "thread copy_to_hdfs"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    char path2[sizeof(RAW_LOG_PATH) + 128];
    struct dirent **namelist = NULL;
    std::deque<std::string>& que = upload_queues[topic];
    pthread_mutex_t& mtx = upload_mutexes[topic];

    do {
        path[n] = '\0';

        int num = scandir(path, &namelist, scandir_filter, scandir_compar);
        if (num == -1) {
            write_log(app_log_path, LOG_ERR, "scandir[%s] failed"
                      " with errno[%d]", path, errno);
        }
        for (int i = 0; i < num; i++) {
            struct dirent *dep = namelist[i];
            if (snprintf(path + n, sizeof(path) - n, "/%s", dep->d_name) < 0) {
                continue;
            }
            if (is_write_finished(path)) {
                if (snprintf(path2, sizeof(path2), "%s%s/%s/%s",
                             CWD_ROOT, UPLOAD_PATH, topic, dep->d_name) < 0) {
                    continue;
                }
                if (rename(path, path2) != 0) {
                    write_log(app_log_path, LOG_ERR, "rename[%s:%s] failed"
                              " with errno[%d]", path, path2, errno);
                } else {
                    if (strcmp(topic, "bid") != 0
                        && strcmp(topic, "unbid") != 0) {
                        cleanup_fp(path);
                    }
                    pthread_mutex_lock(&mtx);
                    que.push_back(path2);
                    pthread_mutex_unlock(&mtx);
                }
            }
            free(dep);
        }
        free(namelist);

        sleep(70);
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread copy_to_hdfs"
              " for topic[%s] exiting", topic);

    return (void *)0;
}

#define POP_NUM 2

static void *copy_to_hdfs(void *arg)
{
    char *topic = (char *)arg;
    write_log(app_log_path, LOG_INFO, "thread copy_to_hdfs"
              " for topic[%s] created", topic);

    int ret = pthread_detach(pthread_self());
    if (ret != 0) {
        write_log(app_log_path, LOG_ERR, "pthread_detach[copy_to_hdfs] failed"
                  " with errno[%d]", ret);
        write_log(app_log_path, LOG_ERR, "thread copy_to_hdfs"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    std::deque<std::string>& que = upload_queues[topic];
    pthread_mutex_t& mtx = upload_mutexes[topic];
    std::string files[POP_NUM];

    do {
        int num = 0;

        // XXX
        pthread_mutex_lock(&mtx);
        while (!que.empty()) {
            files[num++].assign(que.front());
            que.pop_front();
            if (num == POP_NUM) break;
        }
        pthread_mutex_unlock(&mtx);

        for (int i = 0; i < num; i++) {
            const char *f = files[i].c_str();
            if (strcmp(topic, "bid") == 0 || strcmp(topic, "unbid") == 0) {
                if (copy_ex(topic, f) == 0) rm_local(f);
            } else {
                if (copy(topic, f) == 0
                    || copy_ex(topic, f, false) == 0) {
                    rm_local(f);
                }
            }
        }
    } while (true);

    write_log(app_log_path, LOG_ERR, "thread copy_to_hdfs"
              " for topic[%s] exiting unexpected", topic);
    return (void *)-1;
}

static void set_timezone()
{
    extern int daylight;
    daylight = 0;
    tzset();
}

#if 0
static void sa_callback(int signum, siginfo_t *si, void *uc)
{
    write_log(app_log_path, LOG_WARNING, "si_signo[%d] si_errno[%d] si_code[%d]"
              " si_pid[%d] si_status[%d] si_addr[%p]", si->si_signo,
             si->si_errno, si->si_code, si->si_pid, si->si_status, si->si_addr);
}
#endif

static void destroy_upload_mutexes()
{
    for (std::map<std::string, pthread_mutex_t>::iterator it =
         upload_mutexes.begin(); it != upload_mutexes.end(); it++) {
        pthread_mutex_destroy(&it->second);
    }
}

static bool init_upload_queue_mutexes()
{
    for (int i = 0; i < topic_total; i++) {
        pthread_mutex_t mtx;
        int ret = pthread_mutex_init(&mtx, NULL);
        if (ret != 0) {
            write_log(app_log_path, LOG_ERR, "pthread_mutex_init for topic[%s]"
                      " failed with errno[%d]", topics[i], ret);
            return false;
        }
        upload_mutexes[topics[i]] = mtx;
        upload_queues[topics[i]] = std::deque<std::string>();
    }
    return true;
}

static int remedy()
{
    char path[sizeof(UPLOAD_PATH) + 128];
    int n = snprintf(path, sizeof(path), "%s%s", CWD_ROOT, UPLOAD_PATH);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf failed");
        return -1;
    }

    int total = 0;
    struct dirent **namelist = NULL;
    for (int i = 0; i < topic_total; i++) {
        int m = snprintf(path + n, sizeof(path) - n , "/%s", topics[i]);

        int num = scandir(path, &namelist, scandir_filter, scandir_compar);
        if (num == -1) {
            write_log(app_log_path, LOG_ERR, "scandir[%s] failed"
                      " with errno[%d]", path, errno);
            continue;
        }

        std::deque<std::string>& que = upload_queues[topics[i]];

        for (int i = 0; i < num; i++) {
            struct dirent *dep = namelist[i];
            snprintf(path + n + m, sizeof(path) - n - m, "/%s", dep->d_name);
            write_log(app_log_path, LOG_INFO, "remedy[%s]", path);
            que.push_back(path);
            total++;
            free(dep);
        }

        free(namelist);
    }

    return total;
}

int main(int argc, char *argv[])
{
    int opt;
    char *k2h_conf_path, *offset_opt;

    do {
        opt = getopt(argc, argv, "c:k:l:o:");
        switch (opt) {
        case 'c':
            consumer_conf_path = optarg;
            fprintf(stderr, "conf path for Kafka consumer: %s\n",
                    consumer_conf_path);
            break;
        case 'k':
            k2h_conf_path = optarg;
            fprintf(stderr, "conf path for kafka2hdfs: %s\n", k2h_conf_path);
            break;
        case 'l':
            app_log_path = optarg;
            fprintf(stderr, "app log path for kafka2hdfs: %s\n", app_log_path);
            break;
        case 'o':
            offset_opt = optarg;
            fprintf(stderr, "offset option for kafka2hdfs: %s\n", offset_opt);
            break;
        default:
            // XXX
            fprintf(stderr, "Usage: ./kafka2hdfs -c -k\n");
            continue;
        }
    } while (opt != -1);

    if (strcmp(offset_opt, "stored") == 0) {
        start_offset = KAFKA_OFFSET_STORED;
    } else if (strcmp(offset_opt, "beginning") == 0) {
        start_offset = KAFKA_OFFSET_BEGINNING;
    } else if (strcmp(offset_opt, "end") == 0) {
        start_offset = KAFKA_OFFSET_END;
    } else {
        fprintf(stderr, "Invalid offset option[%s], default \"stored\"",
                offset_opt);
    }

#if 0
    struct sigaction act;
    memset(&act, 0, sizeof(struct sigaction));
    act.sa_sigaction = sa_callback;
    if (sigaction(SIGABRT, &act, NULL) != 0) {
        write_log(app_log_path, LOG_ERR,
                  "sigaction failed with errno[%d]", errno);
        exit(EXIT_FAILURE);
    }
#endif

    if (!parse_conf_file(k2h_conf_path)) {
        write_log(app_log_path, LOG_ERR, "parse_conf_file[%s] failed",
                  k2h_conf_path);
        exit(EXIT_FAILURE);
    }

    int thr_total = extract_topics_partnos(k2h_conf["topic-partitions-map"]);
    if (thr_total == 0) {
        write_log(app_log_path, LOG_ERR, "extract_topics_partnos failed");
        exit(EXIT_FAILURE);
    }

    write_log(app_log_path, LOG_INFO, "consuming thread total[%d]", thr_total);

    extract_intervals(k2h_conf["topic-interval-map"]);
#if 1
for (std::map<std::string, int>::const_iterator it =
     upload_intervals.begin(); it != upload_intervals.end(); it++) {
    write_log(app_log_path, LOG_INFO,
              "DEBUG topic[%s] interval[%d]\n",
              it->first.c_str(), it->second);
}
#endif

    init_topic_hdfs_map();

    set_timezone();

    pthread_t *thrs = (pthread_t *)malloc(sizeof(pthread_t) * thr_total);
    if (thrs == NULL) {
        write_log(app_log_path, LOG_ERR, "malloc for threads failed");
        free_topics_partnos();
        exit(EXIT_FAILURE);
    }

    if ((consumer = create_kafka_consumer(consumer_conf_path, NULL)) == NULL) {
        free(thrs);
        free_topics_partnos();
        exit(EXIT_FAILURE);
    }

    if (!init_topics()) {
        free_consumer();
        free(thrs);
        free_topics_partnos();
        exit(EXIT_FAILURE);
    }

    if (!init_upload_queue_mutexes()) {
        destroy_topics();
        free_consumer();
        free(thrs);
        free_topics_partnos();
        exit(EXIT_FAILURE);
    }

    int n = remedy(), ret;
    write_log(app_log_path, LOG_INFO, "%d files remedy", n);

    n = 0;
    for (int i = 0; i < topic_total; i++) {
        for (int j = 0; j < partnos[i]; j++) {
            ThrArg *arg = new ThrArg(kcts[i], j);
            ret = pthread_create(&thrs[n], NULL, consume_to_local, (void *)arg);
            if (ret != 0) {
                write_log(app_log_path, LOG_ERR,
                          "pthread_create[consume_to_local] for "
                          "topic[%s] partition[%d] failed with errno[%d]",
                          topics[i], j, ret);
                clear_fp_cache();
                destroy_topics();
                free_consumer();
                free(thrs);
                free_topics_partnos();
                exit(EXIT_FAILURE);
            }
            n++;
        }

        if (strcmp(topics[i], "bid") == 0 || strcmp(topics[i], "unbid") == 0) {
            pthread_t thr;
            ret = pthread_create(&thr, NULL, compress_files, (void *)topics[i]);
            if (ret != 0) {
                write_log(app_log_path, LOG_ERR,
                          "pthread_create[compress_files] failed"
                          " with errno[%d]", errno);
                clear_fp_cache();
                destroy_topics();
                free_consumer();
                free(thrs);
                free_topics_partnos();
                exit(EXIT_FAILURE);
            }
        }

        pthread_t thr2;
        ret = pthread_create(&thr2, NULL, enqueue_upload_files,
                             (void *)topics[i]);
        if (ret != 0) {
            write_log(app_log_path, LOG_ERR, "pthread_create["
                      "enqueue_upload_files] failed with errno[%d]", errno);
            clear_fp_cache();
            destroy_topics();
            free_consumer();
            free(thrs);
            free_topics_partnos();
            exit(EXIT_FAILURE);
        }

        pthread_t thr3;
        ret = pthread_create(&thr3, NULL, copy_to_hdfs, (void *)topics[i]);
        if (ret != 0) {
            write_log(app_log_path, LOG_ERR, "pthread_create[copy_to_hdfs]"
                      " failed with errno[%d]", errno);
            clear_fp_cache();
            destroy_topics();
            free_consumer();
            free(thrs);
            free_topics_partnos();
            exit(EXIT_FAILURE);
        }
    }

    // XXX
    for (int i = 0; i < n; i++) {
        int ret = pthread_join(thrs[i], NULL);
        if (ret != 0) {
            write_log(app_log_path, LOG_ERR, "pthread_join[consume_to_local@%d]"
                      " failed with errno[%d]", i, ret);
            clear_fp_cache();
            destroy_topics();
            free_consumer();
            free(thrs);
            free_topics_partnos();
            exit(EXIT_FAILURE);
        }
    }

    clear_fp_cache();
    destroy_topics();
    free_consumer();
    free(thrs);
    free_topics_partnos();

    exit(EXIT_SUCCESS);
}
