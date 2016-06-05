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

#if 0
#include "orc/orc-config.hh"
#include "orc/ColumnPrinter.hh"
#endif

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

static int no;

static char *consumer_conf_path;
static std::map<std::string, std::string> k2h_conf;

static int topic_total;
static char **topics = NULL;
static int *partnos = NULL;
static std::map<char *, std::vector<int> > topic_partnos;

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
static const char *hdfs_path_cmReachmax = "/user/data-infra/cm_reachmax";
static const char *hdfs_path_cm_acxiom =
    "/user/data-infra/cookie_mapping/acxiom";
static const char *hdfs_path_cm_adchina =
    "/user/data-infra/cookie_mapping/adchina";
static const char *hdfs_path_cm_admaster =
    "/user/data-infra/cookie_mapping/admaster";
static const char *hdfs_path_cm_ap = "/user/data-infra/cookie_mapping/ap";
static const char *hdfs_path_cm_bfd = "/user/data-infra/cookie_mapping/bfd";
static const char *hdfs_path_cm_bshare =
    "/user/data-infra/cookie_mapping/bshare";
static const char *hdfs_path_cm_cmadv = "/user/data-infra/cookie_mapping/cmadv";
static const char *hdfs_path_cm_dmpmz = "/user/data-infra/cookie_mapping/dmpmz";
static const char *hdfs_path_cm_fingerprint =
    "/user/data-infra/cookie_mapping/fingerprint";
static const char *hdfs_path_cm_gzt = "/user/data-infra/cookie_mapping/gzt";
static const char *hdfs_path_cm_hc360 = "/user/data-infra/cookie_mapping/hc360";
static const char *hdfs_path_cm_idm = "/user/data-infra/cookie_mapping/idm";
static const char *hdfs_path_cm_ifeng = "/user/data-infra/cookie_mapping/ifeng";
static const char *hdfs_path_cm_iresearch =
    "/user/data-infra/cookie_mapping/iresearch";
static const char *hdfs_path_cm_letv = "/user/data-infra/cookie_mapping/letv";
static const char *hdfs_path_cm_miaozhen =
    "/user/data-infra/cookie_mapping/miaozhen";
static const char *hdfs_path_cm_neustar =
    "/user/data-infra/cookie_mapping/neustar";
static const char *hdfs_path_cm_omg = "/user/data-infra/cookie_mapping/omg";
static const char *hdfs_path_cm_reachmax =
    "/user/data-infra/cookie_mapping/reachmax";
static const char *hdfs_path_cm_shuyun =
    "/user/data-infra/cookie_mapping/shuyun";
static const char *hdfs_path_cm_stellar =
    "/user/data-infra/cookie_mapping/stellar";
static const char *hdfs_path_cm_suning =
    "/user/data-infra/cookie_mapping/suning";
static const char *hdfs_path_cm_vivaki =
    "/user/data-infra/cookie_mapping/vivaki";
static const char *hdfs_path_cm_annalect =
    "/user/data-infra/cookie_mapping/annalect";
static const char *hdfs_path_cm_xaxis = "/user/data-infra/cookie_mapping/xaxis";
static const char *hdfs_path_cm_yhd = "/user/data-infra/cookie_mapping/yhd";
static const char *hdfs_path_cm_yxkj = "/user/data-infra/cookie_mapping/yxkj";
static const char *hdfs_path_pydmp_imp = "/user/data-infra/pydmp/imp";
static const char *hdfs_path_pydmp_adv = "/user/data-infra/pydmp/adv";
static const char *hdfs_path_pydmp_click = "/user/data-infra/pydmp/click";
static const char *hdfs_path_pydmp_cvt = "/user/data-infra/pydmp/cvt";
static const char *hdfs_path_tencentdmp = "/user/root/flume/dmp/tencent";

static std::map<const char *, const char *> topic_hdfs_map;

static std::map<std::string, FILE *> fp_cache;
static pthread_rwlock_t fp_lock = PTHREAD_RWLOCK_INITIALIZER;

static char *app_log_path = NULL;

static int64_t start_offset = KAFKA_OFFSET_STORED;

static std::map<std::string, std::deque<std::string> > upload_queues;
static std::map<std::string, pthread_mutex_t> upload_mutexes;

static std::map<std::string, hdfsFS> hdfs_handle_cache;
static pthread_rwlock_t hdfs_handle_lock = PTHREAD_RWLOCK_INITIALIZER;

// TODO optimaztion needed
static bool align_YmdHM(char *YmdHM, int interval)
{
    if (strlen(YmdHM) != 12) return false;

    struct tm r;
    char c;

    r.tm_sec = 0;
    r.tm_min = atoi(YmdHM + 10);
    // reduce small files on HDFS
    if (r.tm_min % interval == 0) r.tm_min += 1;

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
    // XXX
    if (rem == 0) {
        return true;
    }

    // ensure correct hours
    t += interval * 60 - rem - 2;
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

// unbid:60:0-24
// unbid:60:25-59
static void parse_partno_detail(const std::string& partnos, int bound,
                                std::vector<int>& result)
{
    const char *p = strchr(partnos.c_str(), '-');
    if (p == NULL) {
        write_log(app_log_path, LOG_WARNING,
                  "invalide partition number list[%s]", partnos.c_str());
        return;
    }
    int e = atoi(p + 1);
    if (e >= bound) {
        write_log(app_log_path, LOG_WARNING, "invalid right bound[%d]", e);
        e = bound - 1;
    }
    for (int i = atoi(partnos.c_str()); i <= e; i++) {
        result.push_back(i);
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
        if (vec2.size() == 3) {
            std::vector<int> nos;
            parse_partno_detail(vec2[2], m, nos);
            topic_partnos.insert(
                std::pair<char *, std::vector<int> >(topics[i], nos));
        }
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
    if (strcmp(topic, "tencentdmp") == 0) {
        snprintf(YmdHM, 13, "%s", s);
        return true;
    }

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

    FILE *fp = NULL;

    pthread_rwlock_wrlock(&fp_lock);
    std::map<std::string, FILE *>::iterator it = fp_cache.find(bn);
    if (it == fp_cache.end()) {
        write_log(app_log_path, LOG_WARNING,
                  "invalid key[%s] for fp_cache", bn.c_str());
    } else {
        fp = it->second;
        fp_cache.erase(it);
    }
    pthread_rwlock_unlock(&fp_lock);

    if (fp != NULL) {
        if (fclose(fp) != 0) {
            write_log(app_log_path, LOG_ERR,
                      "fclose[%s] failed with errno[%d]", filename, errno);
        } else {
            write_log(app_log_path, LOG_INFO, "DEBUG fclose[%s] OK", filename);
        }
    } else {
        write_log(app_log_path, LOG_WARNING, "unexpected null FP[%s]", filename);
    }
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
            write_log(app_log_path, LOG_WARNING,
                      "extract_device[%s] topic[%s] failed", payload, topic);
            return false;
        }
        snprintf(path, 128, "%s%s%d/%s/%s_%ld_%s_%d.seq",
                 CWD_ROOT, RAW_LOG_PATH, no, topic, topic, ymdhm, device, no);
    } else if (strcmp(topic, "imp") == 0 || strcmp(topic, "ic") == 0) {
        int actiontype = extract_action_type(payload);
        switch (actiontype) {
        case 1: // impression
            snprintf(path, 128, "%s%s%d/%s/imp_%ld_%d.seq",
                     CWD_ROOT, RAW_LOG_PATH, no, topic, ymdhm, no);
            break;
        case 2: // click
            snprintf(path, 128, "%s%s%d/%s/click_%ld_%d.seq",
                     CWD_ROOT, RAW_LOG_PATH, no, topic, ymdhm, no);
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
            snprintf(path, 128, "%s%s%d/%s/imp_%ld_%d.seq",
                     CWD_ROOT, RAW_LOG_PATH, no, topic, ymdhm, no);
            break;
        case 12: // click
            snprintf(path, 128, "%s%s%d/%s/click_%ld_%d.seq",
                     CWD_ROOT, RAW_LOG_PATH, no, topic, ymdhm, no);
            break;
        default: // invalid for ic or stats
            write_log(app_log_path, LOG_WARNING,
                      "invalid action type[%d] for topic[stats]msg[%s]",
                      actiontype, payload);
            return false;
        }
    } else {
        snprintf(path, 128, "%s%s%d/%s/%s_%ld_%d.seq",
                 CWD_ROOT, RAW_LOG_PATH, no, topic, topic, ymdhm, no);
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

    // XXX
    char temp[len + 1];
    memcpy(temp, payload, len);
    temp[len] = '\n';

    size_t n = fwrite(temp, 1, len + 1, fp);
    if (n != len + 1) {
        write_log(app_log_path, LOG_WARNING, "fwrite[%s] might fail: "
                  "written[%lu] expected[%lu]", loc_path, n, len + 1);
        if ((fp = retrieve_fp(loc_path)) == NULL) {
            write_log(app_log_path, LOG_ERR,
                      "retrieve_fp[%s] failed", loc_path);
            return false;
        }
        // XXX non-atomic
        size_t n2 = fwrite(temp + n, 1, len + 1 - n, fp);
        if (n2 != len + 1 - n) {
            write_log(app_log_path, LOG_ERR, "fwrite[%s] failed: "
                      "written[%lu] expected[%lu]", loc_path, n2, len + 1 - n);
            return false;
        }
    }

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

static off_t get_file_size(const char *path)
{
    struct stat stbuf;
    if (stat(path, &stbuf) != 0) {
        write_log(app_log_path, LOG_ERR,
                  "stat[%s] for size failed with errno[%d]", path, errno);
        return -1;
    }
    return stbuf.st_size;
}

#define BUFFER_MINUTES 3

//static bool is_write_finished(const char *path, const char *topic)
static bool is_write_finished(const char *path, int minutes = BUFFER_MINUTES)
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
    return (time(NULL) - mt) > minutes * 60;
}

static inline void rm_local(const char *f, int flag = 1, const char *bu = NULL)
{
    switch (flag) {
    case 1: // delete
        if (unlink(f) != 0) {
            write_log(app_log_path, LOG_WARNING,
                      "unlink[%s] failed with errno[%d]", f, errno);
        } else {
            write_log(app_log_path, LOG_INFO, "DEBUG unlink[%s] OK", f);
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
    //snprintf(cmd, sizeof(cmd), "%s %s", LZOP, in_path);
    snprintf(cmd, sizeof(cmd), "%s %s &", LZOP, in_path);
    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define MAX_FILE_SIZE 6979321856 // 6.5G

static int scandir_filter_2(const struct dirent *dep)
{
    if (strcmp(dep->d_name, ".") == 0 || strcmp(dep->d_name, "..") == 0) {
        return 0;
    }
    const char *p = strrchr(dep->d_name, '.') + 1;
    return strncmp(p, "seq", 3) == 0 ? 1 : 0;
}

static int scandir_compar_2(const struct dirent **a, const struct dirent **b)
{
    return strcmp((*a)->d_name, (*b)->d_name);
}

// bid and unbid only need to be compressed
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
    int n = snprintf(path, sizeof(path), "%s%s%d/%s",
                     CWD_ROOT, RAW_LOG_PATH, no, topic);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf topic[%s] failed", topic);
        write_log(app_log_path, LOG_ERR, "thread compress_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }
    memcpy(path2, path, n);

    struct dirent **namelist = NULL;

    do {
        path[n] = '\0';

        int num = scandir(path, &namelist, scandir_filter_2, scandir_compar_2);
        if (num == -1) {
            write_log(app_log_path, LOG_ERR, "scandir[%s] failed"
                      " with errno[%d]", path, errno);
        }
        for (int i = 0; i < num; i++) {
            struct dirent *dep = namelist[i];
            if (snprintf(path + n, sizeof(path) - n, "/%s", dep->d_name) < 0) {
                free(dep);
                continue;
            }

            if (is_write_finished(path)) {
                snprintf(path2 + n, sizeof(path2) - n, "/%s.0", dep->d_name);
                write_log(app_log_path, LOG_INFO,
                          "DEBUG path[%s] writing finished", path2);
            } else if (get_file_size(path) > MAX_FILE_SIZE) {
                snprintf(path2 + n, sizeof(path2) - n, "/%s.%ld",
                         dep->d_name, time(NULL));
                write_log(app_log_path, LOG_INFO, "DEBUG path[%s] too large", path2);
            } else {
                free(dep);
                continue;
            }
            // XXX
            if (rename(path, path2) != 0) {
                write_log(app_log_path, LOG_ERR, "rename[%s to %s] failed"
                          " with errno[%d]", path, path2, errno);
                free(dep);
                continue;
            }
            cleanup_fp(path);
            // TODO non-blocking exit code
            lzo_compress(path2);

            free(dep);
        }
        free(namelist);

        sleep(70);
    } while (true);

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
        } else if (strcmp(topic, "cm_reachmax") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cmReachmax;
        } else if (strcmp(topic, "idm") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_idm;
        } else if (strcmp(topic, "miaozhen") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_miaozhen;
        } else if (strcmp(topic, "admaster") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_admaster;
        } else if (strcmp(topic, "letv") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_letv;
        } else if (strcmp(topic, "xaxis") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_xaxis;
        } else if (strcmp(topic, "dmpmz") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_dmpmz;
        } else if (strcmp(topic, "ifeng") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_ifeng;
        } else if (strcmp(topic, "suning") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_suning;
        } else if (strcmp(topic, "reachmax") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_reachmax;
        } else if (strcmp(topic, "cmadv") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_cmadv;
        } else if (strcmp(topic, "vivaki") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_vivaki;
        } else if (strcmp(topic, "acxiom") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_acxiom;
        } else if (strcmp(topic, "yhd") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_yhd;
        } else if (strcmp(topic, "yxkj") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_yxkj;
        } else if (strcmp(topic, "bfd") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_bfd;
        } else if (strcmp(topic, "neustar") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_neustar;
        } else if (strcmp(topic, "gzt") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_gzt;
        } else if (strcmp(topic, "hc360") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_hc360;
        } else if (strcmp(topic, "ap") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_ap;
        } else if (strcmp(topic, "iresearch") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_iresearch;
        } else if (strcmp(topic, "adchina") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_adchina;
        } else if (strcmp(topic, "bshare") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_bshare;
        } else if (strcmp(topic, "fingerprint") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_fingerprint;
        } else if (strcmp(topic, "omg") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_omg;
        } else if (strcmp(topic, "shuyun") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_shuyun;
        } else if (strcmp(topic, "stellar") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_stellar;
        } else if (strcmp(topic, "annalect") == 0) {
            topic_hdfs_map[topic] = hdfs_path_cm_annalect;
        } else if (strcmp(topic, "pydmpimp") == 0) {
            topic_hdfs_map[topic] = hdfs_path_pydmp_imp;
        } else if (strcmp(topic, "pydmpadv") == 0) {
            topic_hdfs_map[topic] = hdfs_path_pydmp_adv;
        } else if (strcmp(topic, "pydmpclick") == 0) {
            topic_hdfs_map[topic] = hdfs_path_pydmp_click;
        } else if (strcmp(topic, "pydmpcvt") == 0) {
            topic_hdfs_map[topic] = hdfs_path_pydmp_cvt;
        } else if (strcmp(topic, "tencentdmp") == 0) {
            topic_hdfs_map[topic] = hdfs_path_tencentdmp;
        }
    }
}

// XXX underscore character in bn or topic
static bool build_hdfs_path(const char *topic, const char *bn, char *path)
{
    const char *p;
    if (strncmp(topic, bn, strlen(topic)) == 0) {
        p = strchr(bn + strlen(topic), '_');
    } else {
        p = strchr(bn, '_');
    }
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

    // length of Ymd is 12
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

#define HADOOP_LZO_INDEX "hadoop jar /usr/lib/hadoop/lib/" \
    "hadoop-lzo-0.4.19.jar com.hadoop.compression.lzo.LzoIndexer"

#define HDFS_PUT "hadoop fs -put"

// TODO for large files to be uploaded
// should be implemented using libhdfs directly
// referring Impala
static int hdfs_put(const char *loc_path, const char *hdfs_path,
                    bool deleted = true, bool indexed = false)
{
    char cmd[512];
    int n, n2;

    n = snprintf(cmd, sizeof(cmd), "%s %s %s", HDFS_PUT, loc_path, hdfs_path);
    if (deleted) {
        n2 = snprintf(cmd + n, sizeof(cmd) - n, " && rm -f %s", loc_path);
        n += n2;
    }
    if (indexed) {
        n2 = snprintf(cmd + n, sizeof(cmd) - n, " && %s %s",
                      HADOOP_LZO_INDEX, hdfs_path);
        n += n2;
    }
    snprintf(cmd + n, sizeof(cmd) - n, " &");

    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define HDFS_APPEND "hadoop fs -appendToFile"

static int hdfs_append(const char *loc_path, const char *hdfs_path,
                       bool deleted = true, bool indexed = false)
{
    char cmd[512];
    int n, n2;

    n = snprintf(cmd, sizeof(cmd), "%s %s %s",
                 HDFS_APPEND, loc_path, hdfs_path);
    if (deleted) {
        n2 = snprintf(cmd + n, sizeof(cmd) - n, " && rm -f %s", loc_path);
        n += n2;
    }
    if (indexed) {
        n2 = snprintf(cmd + n, sizeof(cmd) - n, " && %s %s",
                      HADOOP_LZO_INDEX, hdfs_path);
        n += n2;
    }
    snprintf(cmd + n, sizeof(cmd) - n, " &");

    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

static int hdfs_lzo_index(const char *hdfs_path)
{
    char cmd[512];
    //snprintf(cmd, sizeof(cmd), "%s %s", HADOOP_LZO_INDEX, hdfs_path);
    snprintf(cmd, sizeof(cmd), "%s %s &", HADOOP_LZO_INDEX, hdfs_path);
    write_log(app_log_path, LOG_INFO, "DEBUG exec-ing cmd[%s]", cmd);
    return wrap_system(cmd);
}

#define HDFS_USER "data-infra"

static int init_hdfs_handle_cache()
{
    char handle_key[256];
    snprintf(handle_key, sizeof(handle_key), "%s^%s^%s",
             k2h_conf["namenode"].c_str(), k2h_conf["port"].c_str(), HDFS_USER);

    pthread_rwlock_wrlock(&hdfs_handle_lock);

    std::map<std::string, hdfsFS>::iterator it =
        hdfs_handle_cache.find(handle_key);
    if (it != hdfs_handle_cache.end()) {
        if (it->second != NULL) {
            if (hdfsDisconnect(it->second) != 0) {
                write_log(app_log_path, LOG_WARNING, "hdfsDisconnect failed");
            }
            it->second = NULL;
        }
    }

    hdfsFS fs_handle = hdfsConnectAsUser(k2h_conf["namenode"].c_str(),
                                         atoi(k2h_conf["port"].c_str()),
                                         HDFS_USER);
    if (fs_handle == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "hdfsConnect[%s] failed with errno[%d]",
                  handle_key, errno);
        pthread_rwlock_unlock(&hdfs_handle_lock);
        return -1;
    }

    if (it != hdfs_handle_cache.end()) {
        it->second = fs_handle;
    } else {
        hdfs_handle_cache.insert(std::pair<std::string, hdfsFS>(handle_key,
                                                                fs_handle));
    }

    pthread_rwlock_unlock(&hdfs_handle_lock);

    return 1;
}

static void destroy_hdfs_handle_cache()
{
    pthread_rwlock_wrlock(&hdfs_handle_lock);

    for (std::map<std::string, hdfsFS>::iterator it = hdfs_handle_cache.begin();
         it != hdfs_handle_cache.end(); it++) {
        if (it->second != NULL) {
            if (hdfsDisconnect(it->second) != 0) {
                write_log(app_log_path, LOG_WARNING, "hdfsDisconnect failed");
            }
            it->second = NULL;
        }
    }

    pthread_rwlock_unlock(&hdfs_handle_lock);
}

static hdfsFS get_hdfs_handle(const char *key = NULL)
{
    hdfsFS handle;
    pthread_rwlock_rdlock(&hdfs_handle_lock);

    if (key != NULL) {
        std::map<std::string, hdfsFS>::iterator it =
            hdfs_handle_cache.find(key);
        if (it == hdfs_handle_cache.end()) {
            write_log(app_log_path, LOG_WARNING,
                      "hdfs_handle_cache no such key[%s]", key);
            handle = NULL;
        } else {
            handle = it->second;
        }
    } else {
        handle = hdfs_handle_cache.begin()->second;
    }

    pthread_rwlock_unlock(&hdfs_handle_lock);
    return handle;
}

static int copy(const char *topic, const char *zfile)
{
    char hdfs_path[256];
    if (!build_hdfs_path(topic, basename(zfile), hdfs_path)) {
        return -1;
    }

    int retries = 0;
    hdfsFS fs_handle = get_hdfs_handle();
    while (fs_handle == NULL) {
        if (init_hdfs_handle_cache() == -1) return -1;
        fs_handle = get_hdfs_handle();
        if (++retries == 3) {
            write_log(app_log_path, LOG_ERR,
                      "get_hdfs_handle failed after %d retries", retries);
            return -1;
        }
    }

    struct stat st;
    if (stat(zfile, &st) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "stat[%s] failed with errno[%d]", zfile, errno);
        return -1;
    }

    bool existing = hdfsExists(fs_handle, hdfs_path) == 0;
    if (existing) {
        write_log(app_log_path, LOG_INFO, "HDFS[%s] exists already", hdfs_path);
    }

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
                  "hdfsOpenFile[%s] failed with errno[%d]", hdfs_path, errno);
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
    if (ret == 0) rm_local(zfile);
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
    if (wrap_system(cmd, false) == 0) {
        write_log(app_log_path, LOG_WARNING,
                  "HDFS file[%s] already exists", hdfs_path);
        size_t l = strlen(hdfs_path);
        if (strncmp(hdfs_path + l - 4, ".seq", 4) == 0) {
            if (hdfs_append(zfile, hdfs_path, true, indexed) != 0) {
                return -1;
            } else {
                return 0;
            }
        } else {
            do {
                p = strrchr(hdfs_path, '.');
                if (*(p - 2) == '.') {
                    // files whose names end with ".0.lzo"
                    char c = *(p - 1) + 1;
                    *(p - 1) = c;
                    snprintf(cmd, sizeof(cmd),
                             "hadoop fs -test -e %s", hdfs_path);
                } else {
                    // seq.1461669096.lzo
                    memcpy(p + 1, "0.lzo", sizeof("0.lzo") - 1);
                    snprintf(cmd, sizeof(cmd),
                             "hadoop fs -test -e %s", hdfs_path);
                }
            } while (wrap_system(cmd, false) == 0);
        }
    }
    if (hdfs_put(zfile, hdfs_path, true, indexed) != 0) {
        return -1;
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

#if 0
static bool generate_orc_file(const char *src, const orc::ReaderOptions opts,
                              const char *dest)
{
    FILE *fp = fopen(dest, "a");
    if (fp == NULL) {
        write_log(app_log_path, LOG_ERR,
                  "fopen[%s] failed with errno[%d]", dest, errno);
        return false;
    }

    std::unique_ptr<orc::Reader> reader;
    try {
        orc::createReader(orc::readLocalFile(std::string(src)), opts);
    } catch (std::exception& e) {
        write_log(app_log_path, LOG_ERR,
                  "createReader failed with exception[%s]", e.what());
        return false;
    }

    std::unique_ptr<orc::ColumnVectorBatch> batch =
        reader->createRowBatch(1000);
    std::string line;
    std::unique_ptr<orc::ColumnPrinter> printer =
        createColumnPrinter(line, &reader->getSelectedType());

    while (reader->next(*batch)) {
        printer->reset(*batch);
        for (unsigned long i = 0; i < batch->numElements; i++) {
            line.clear();
            printer->printRow(i);
            line += "\n";
            if (fwrite(line.c_str(), 1, line.length(), fp) != line.length()) {
                write_log(app_log_path, LOG_ERR, "fwrite[%s] failed", dest);
                fclose(fp);
                return false;
            }
        }
    }

    fclose(fp);
    return true;
}
#endif

static bool file_exists(const char *path)
{
    struct stat stbuf;
    if (stat(path, &stbuf) != 0) {
        return errno != ENOENT;
    }
    return true;
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
    int n = snprintf(path, sizeof(path), "%s%s%d/%s",
                     CWD_ROOT, RAW_LOG_PATH, no, topic);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf failed");
        write_log(app_log_path, LOG_ERR, "thread enqueue_upload_files"
                  " for topic[%s] exiting", topic);
        return (void *)-1;
    }

    bool bid_related = strcmp(topic, "unbid") == 0 || strcmp(topic, "bid") == 0;
    int buffer_minutes = bid_related ? 1 : BUFFER_MINUTES;
    char path2[sizeof(RAW_LOG_PATH) + 128], path3[sizeof(RAW_LOG_PATH) + 128];
    struct dirent **namelist = NULL;
    std::deque<std::string>& que = upload_queues[topic];
    pthread_mutex_t& mtx = upload_mutexes[topic];
    //orc::ReaderOptions opts;

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
                free(dep);
                continue;
            }
            if (is_write_finished(path, buffer_minutes)) {
                // XXX
                if (!bid_related) cleanup_fp(path);
                long l = get_file_size(path);
                if (l < 64) {
                    if (l != -1) {
                        write_log(app_log_path, LOG_WARNING,
                                  "file[%s:%ld] too small", path, l);
                        rm_local(path);
                    }
                    free(dep);
                    continue;
                }
                int n2 = snprintf(path2, sizeof(path2), "%s%s%d/%s/%s",
                                  CWD_ROOT, UPLOAD_PATH, no,
                                  topic, dep->d_name);
                if (n2 < 0) {
                    write_log(app_log_path, LOG_ERR,
                              "snprintf[%s] to rename failed", dep->d_name);
                    free(dep);
                    continue;
                }
                char *p, suffix = 'a';
                while (file_exists(path2)) {
                    if (suffix == 'a') {
                        p = strrchr(path2, '.');
                        memmove(p + 1, p, strlen(p));
                    }
                    *p = suffix;
                    suffix++;
                }
                if (rename(path, path2) != 0) {
                    write_log(app_log_path, LOG_ERR, "rename[%s:%s] failed"
                              " with errno[%d]", path, path2, errno);
                } else {
                    bool orc_ok = false;
                    if (!bid_related) {
                        strncpy(path3, path2, n2 - 3);
                        path3[n2 - 3] = 'o';
                        path3[n2 - 2] = 'r';
                        path3[n2 - 1] = 'c';
                        // ORC files
                        //orc_ok = generate_orc_file(path2, opts, path3);
                    }
                    pthread_mutex_lock(&mtx);
                    que.push_back(path2);
                    if (orc_ok) que.push_back(path3);
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

#define UPLOAD_BUFFER_MINUTES 20

static int scandir_filter_4(const struct dirent *dep)
{
    if (strcmp(dep->d_name, ".") == 0 || strcmp(dep->d_name, "..") == 0) {
        return 0;
    }
    struct stat stbuf;
    if (stat(dep->d_name, &stbuf) != 0) {
        // XXX
        if (errno != ENOENT) {
            write_log(app_log_path, LOG_ERR, "stat[%s] for mtime failed"
                      " with errno[%d]", dep->d_name, errno);
        }
        return 0;
    }
    return (time(NULL) - stbuf.st_mtime >= UPLOAD_BUFFER_MINUTES * 60) ? 1 : 0;
}

static int handle_delayed_upload(const char *topic)
{
    time_t ts;
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        write_log(app_log_path, LOG_ERR,
                  "gettimeofday failed with errno[%d]", errno);
        return -1;
    } else {
        // roughly every 30 minutes
        if (tv.tv_sec % 1800 == 0 && tv.tv_usec % 200000 == 0) {
            ts = tv.tv_sec;
            write_log(app_log_path, LOG_INFO,
                      "doing[@%ld] handle_delayed_upload", ts);
        } else {
            return 0;
        }
    }

    char cwd[128], temp[256];

    // should always be /data/users/data-infra/kafka2hdfs
    if (getcwd(cwd, sizeof(cwd)) == NULL) {
        write_log(app_log_path, LOG_ERR, "getcwd failed with errno[%d]", errno);
        return -1;
    }
    snprintf(temp, sizeof(temp), "%supload%d/%s", CWD_ROOT, no, topic);
    if (chdir(temp) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "chdir[%s] failed with errno[%d]", temp, errno);
        return -1;
    }

    struct dirent **namelist = NULL;
    int num = scandir(temp, &namelist, scandir_filter_4, scandir_compar);
    if (num == -1) {
        write_log(app_log_path, LOG_ERR,
                  "scandir[%s] failed with errno[%d]", temp, errno);
        chdir(cwd);
        return -1;
    } else if (num > 0) {
        int n = snprintf(temp, sizeof(temp), "%sk2h_log%d/%s",
                         CWD_ROOT, no, topic);
        for (int i = 0; i < num; i++) {
            struct dirent *dep = namelist[i];
            snprintf(temp + n, sizeof(temp) - n, "/%s", dep->d_name);
            if (rename(dep->d_name, temp) == 0) {
                time_t ts2 = ts - (BUFFER_MINUTES + 1) * 60;
                struct timeval times[2];
                times[0].tv_sec = ts2;
                times[0].tv_usec = 0;
                times[1].tv_sec = ts2;
                times[1].tv_usec = 0;
                if (utimes(temp, times) != 0) {
                    write_log(app_log_path, LOG_ERR,
                              "utimes[%s] failed with errno[%d]", temp, errno);
                } else {
                    write_log(app_log_path, LOG_INFO,
                              "utimes[%s] time[%ld]", temp, ts2);
                }
            }
            write_log(app_log_path, LOG_INFO,
                      "recovering delayed file[%s]", temp);
            free(dep);
        }
    }
    free(namelist);

    if (chdir(cwd) == -1) {
        write_log(app_log_path, LOG_ERR,
                  "chdir[%s] failed with errno[%d]", cwd, errno);
        return -1;
    }

    return num;
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

    bool bid_related = strcmp(topic, "bid") == 0 || strcmp(topic, "unbid") == 0;
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
            if (bid_related) {
                if (copy_ex(topic, f) != 0) {
                    write_log(app_log_path, LOG_ERR,
                              "copy_ex[%s] to HDFS failed", f);
                }
            } else {
                if (copy(topic, f) != 0) {
                    if (copy_ex(topic, f, false) != 0) {
                        write_log(app_log_path, LOG_ERR,
                                  "copy[%s] to HDFS failed", f);
                    }
                }
            }
        }

        // XXX
        handle_delayed_upload(topic);
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

static int scandir_filter_3(const struct dirent *dep)
{
    if (strcmp(dep->d_name, ".") == 0 || strcmp(dep->d_name, "..") == 0) {
        return 0;
    }
    const char *p = strrchr(dep->d_name, '.') + 1;
    return isdigit(*p) ? 1 : 0;
}

static int remedy()
{
    char path[sizeof(RAW_LOG_PATH) + 128];
    int n = snprintf(path, sizeof(path), "%s%s%d", CWD_ROOT, RAW_LOG_PATH, no);
    if (n < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf failed in remedy");
        return -1;
    }

    int total1 = 0, total2 = 0;
    struct dirent **namelist = NULL;

    for (int i = 0; i < topic_total; i++) {
        int m = snprintf(path + n, sizeof(path) - n , "/%s", topics[i]);

        int num = scandir(path, &namelist, scandir_filter_3, scandir_compar_2);
        if (num == -1) {
            write_log(app_log_path, LOG_ERR, "scandir[%s] failed"
                      " with errno[%d]", path, errno);
            continue;
        }
        for (int i = 0; i < num; i++) {
            struct dirent *dep = namelist[i];
            if (snprintf(path + n + m, sizeof(path) - n - m,
                         "/%s", dep->d_name) < 0) {
                free(dep);
                continue;
            }
            write_log(app_log_path, LOG_INFO, "remedy[%s]", path);
            // TODO non-blocking exit code
            lzo_compress(path);
            total1++;
            free(dep);
        }

        free(namelist);
    }

    write_log(app_log_path, LOG_INFO, "compressing %d files for remedy", total1);

    if ((n = snprintf(path, sizeof(path), "%s%s%d",
                      CWD_ROOT, UPLOAD_PATH, no)) < 0) {
        write_log(app_log_path, LOG_ERR, "snprintf failed");
        return -1;
    }

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
            total2++;
            free(dep);
        }

        free(namelist);
    }

    return total1 + total2;
}

int main(int argc, char *argv[])
{
    int opt;
    char *k2h_conf_path, *offset_opt;

    do {
        opt = getopt(argc, argv, "c:k:l:o:n:");
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
        case 'n':
            no = atoi(optarg);
            fprintf(stderr, "process no.: %d\n", no);
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
              "DEBUG topic[%s] interval[%d]",
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

    if (init_hdfs_handle_cache() < 1) {
        write_log(app_log_path, LOG_ERR, "init_hdfs_handle_cache failed");
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
        std::map<char *, std::vector<int> >::iterator it =
            topic_partnos.find(topics[i]);
        if (it == topic_partnos.end()) {
            for (int j = 0; j < partnos[i]; j++) {
                ThrArg *arg = new ThrArg(kcts[i], j);
                ret = pthread_create(&thrs[n], NULL, consume_to_local,
                                     (void *)arg);
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
        } else {
            std::vector<int>& nos = it->second;
            for (size_t j = 0; j < nos.size(); j++) {
                ThrArg *arg = new ThrArg(kcts[i], nos[j]);
                ret = pthread_create(&thrs[n], NULL, consume_to_local,
                                     (void *)arg);
                if (ret != 0) {
                    write_log(app_log_path, LOG_ERR,
                              "pthread_create[consume_to_local] for "
                              "topic[%s] partition[%d] failed with errno[%d]",
                              topics[i], nos[j], ret);
                    clear_fp_cache();
                    destroy_topics();
                    free_consumer();
                    free(thrs);
                    free_topics_partnos();
                    exit(EXIT_FAILURE);
                }
                n++;
            }
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
    destroy_hdfs_handle_cache();

    exit(EXIT_SUCCESS);
}
