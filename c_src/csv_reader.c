// Authorship lost, no copyrights
#include <stdint.h>
#include "erl_nif.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

#define MAX_FIELDS 100

typedef enum { 
  NO_FILTER = 0,
  INT_FILTER,
  FLOAT_FILTER,
  CENTS_FILTER,
  DATE_FILTER,
  TIME_FILTER,
  GMT_OFFSET_FILTER
} Filter;

typedef struct {
  int fd;
  size_t len;
  char *start, *ptr;
  int map[MAX_FIELDS];
  Filter filters[MAX_FIELDS];
  int filters_addons[MAX_FIELDS];
  int out_size;
  ERL_NIF_TERM header;
} CSV;

ErlNifResourceType *CSVResource;

#define PATH_SIZE 1024

static void 
csv_destructor(ErlNifEnv* env, void *obj)
{
  CSV *csv = (CSV *)obj;
  if(csv->fd > 0) close(csv->fd);
  if(csv->start) munmap(csv->start, csv->len);
  csv->start = NULL;
  csv->fd = -1;
}

static int
load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
  CSVResource = enif_open_resource_type(env, NULL, "csv_resource", csv_destructor, ERL_NIF_RT_CREATE|ERL_NIF_RT_TAKEOVER, NULL);
  return 0;
}
// 
// static ERL_NIF_TERM
// csv_open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
// {
//   CSV *csv;
//   char path[PATH_SIZE];
//   int arity;
//   ERL_NIF_TERM opt, opts, *kv;
//   
//   
//   if (enif_get_string(env, argv[0], path, sizeof(path), ERL_NIF_LATIN1) <= 0) {
//     return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, "Should pass string filename", ERL_NIF_LATIN1));
//   }
//   if (!enif_is_list(env, argv[1])) {
//     return enif_make_badarg(env);
//   }
//   
//   csv = (CSV *)enif_alloc_resource(CSVResource, sizeof(CSV));
//   if(!csv) {
//     return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, "Couldn't allocate csv resource", ERL_NIF_LATIN1));
//   }
//   bzero(csv, sizeof(CSV));
//   {
//     int i;
//     for(i = 0; i < MAX_FIELDS; i++) {
//       csv->filters_addons[i] = -1;
//     }
//   }
//   csv->fd = open(path, O_RDONLY);
//   if(csv->fd < 0) {
//     enif_release_resource(csv);
//     return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, "Couldn't open file", ERL_NIF_LATIN1));
//   }
//   struct stat st;
//   fstat(csv->fd, &st);
//   csv->len = (size_t)st.st_size;
//   if((csv->start = mmap(NULL, csv->len, PROT_READ, MAP_SHARED, csv->fd, 0)) == MAP_FAILED) {
//     close(csv->fd);
//     csv->fd = -1;
//     enif_release_resource(csv);
//     return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_string(env, "Couldn't mmap file", ERL_NIF_LATIN1));    
//   }
//   csv->ptr = csv->start;
//   
//   char buf[2048];
//   char *eol = strchr(csv->ptr, '\n');
//   strncpy(buf, csv->ptr, eol - csv->ptr);
//   csv->ptr = eol + 1;
//   while((buf[strlen(buf)] == '\r' || buf[strlen(buf)] == '\n') && strlen(buf) > 0) {
//     buf[strlen(buf)] = 0;
//   }
//   
//   char names[MAX_FIELDS][100];
//   bzero(names, sizeof(names));
//   
//   char *ptr = buf;
//   if(ptr[0] == '#') ptr++;
//   char *token;
//   int i = 0;
//   while((token = strsep(&ptr, ","))) {
//     strncpy(names[i], token, sizeof(names[i]));
//     char *name_end = &names[i][strlen(names[i]) - 1];
//     while(name_end > names[i] && (*name_end == ' ' || *name_end == '\r' || *name_end == '\n')) {
//       *name_end = 0;
//       name_end--;
//     }
//     
//     // fprintf(stderr, "Adding CSV field '%s'\r\n", names[i]);
//     i++;
//     if(i == MAX_FIELDS - 1) {
//       close(csv->fd);
//       munmap(csv->start, csv->len);
//       csv->start = NULL;
//       return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "too_many_fields"));
//     }
//   }
//   
//   opts = argv[1];
//   while(enif_get_list_cell(env, opts, &opt, &opts)) {
//     if(enif_is_tuple(env, opt)) {
//       enif_get_tuple(env, opt, &arity, (const ERL_NIF_TERM **)&kv);
//       if(arity < 2) continue;
//       
//       char name[100];
//       
//       if(enif_get_string(env, kv[0], name, sizeof(name), ERL_NIF_LATIN1) > 0) {
//         for(i = 0; names[i][0]; i++) {
//           if(!strcmp(names[i], name)) break;
//           // fprintf(stderr, "name: '%s' '%s'\r\n", name, names[i]);
//         }
//         
//         if(!names[i][0]) {
//           // fprintf(stderr, "Skip unknown mapping '%s'\r\n", name);
//           continue;
//         }
//         
//         enif_get_int(env, kv[1], &csv->map[i]);
//         csv->map[i]--;
//         if(arity >= 3) {
//           char filter_name[100];
//           if(enif_get_atom(env, kv[2], filter_name, sizeof(filter_name), ERL_NIF_LATIN1) > 0) {
//             if(!strcmp(filter_name, "int")) csv->filters[i] = INT_FILTER;
//             if(!strcmp(filter_name, "float")) csv->filters[i] = FLOAT_FILTER;
//             if(!strcmp(filter_name, "cents")) csv->filters[i] = CENTS_FILTER;
//             if(!strcmp(filter_name, "date")) csv->filters[i] = DATE_FILTER;
//             if(!strcmp(filter_name, "time")) csv->filters[i] = TIME_FILTER;
//             if(!strcmp(filter_name, "gmt_offset")) csv->filters[i] = GMT_OFFSET_FILTER;
//           }
//           if(arity >= 4) {
//             int j;
//             char addon[1024];
//             if(enif_get_string(env, kv[3], addon, sizeof(addon), ERL_NIF_LATIN1) > 0) {
//               for(j = 0; names[j][0]; j++) {
//                 if(!strcmp(names[j], addon)) {
//                   csv->filters_addons[i] = j;
//                   break;
//                 }
//               }
//             }
//           }
//         }
//         // fprintf(stderr, "'%s'(%d) -> %d\r\n", name, i, csv->map[i]);
//         continue;
//       }
//       
//       if(!enif_compare(kv[0], enif_make_atom(env, "header"))) {
//         csv->header = kv[1];
//       }
// 
//       if(!enif_compare(kv[0], enif_make_atom(env, "size"))) {
//         enif_get_int(env, kv[1], &csv->out_size);
//       }
//       // if(!enif_compare(kv[0], enif_make_atom(env, "tune"))) {
//       //     if(enif_is_list(env, kv[1])) {
//       //         enif_get_string(env, kv[1], x264->tune, sizeof(x264->tune), ERL_NIF_LATIN1);
//       //     }
//       // }
//     }
//   }
// 
//   
//   
//   ERL_NIF_TERM res = enif_make_resource(env, (void *)csv);
//   enif_release_resource(csv);
//   
//   return enif_make_tuple2(env, enif_make_atom(env, "ok"), res);
// }
// 
// static ERL_NIF_TERM
// csv_next(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
// {
//   CSV *csv;
//   char buf[2048];
//   
//   if(!enif_get_resource(env, argv[0], CSVResource, (void **)&csv)) {
//     return enif_make_badarg(env);
//   }
//   
//   if(csv->len - (csv->ptr - csv->start) < 10) {
//     return enif_make_atom(env, "undefined");
//   }
//   
//   char *eol = strchr(csv->ptr, '\n');
//   int len = eol - csv->ptr;
//   strncpy(buf, csv->ptr, len);
//   csv->ptr = eol + 1;
//   
//   while(len > 0 && (buf[len - 1] == '\r' || buf[len-1] == '\n')) {
//     buf[len -1] = 0;
//     len--;
//   }
//   
//   ERL_NIF_TERM reply[csv->out_size];
//   int i = 0;
//   for(i = 1; i < csv->out_size; i++) {
//     reply[i] = enif_make_atom(env, "undefined");
//   }
//   reply[0] = csv->header;
//   
//   
//   char *tokens[MAX_FIELDS];
//   bzero(tokens, sizeof(tokens));
//   
//   char *ptr = buf;
//   for(i = 0; ;i++) {
//     char *token = strsep(&ptr, ",");
//     if(!token) break;
//     tokens[i] = token;
//   }
//   for(i = 0; tokens[i];i++) {
//     char *token = tokens[i];
//     if(token[0] && csv->map[i])  {
//       if(csv->filters[i] == INT_FILTER) {
//         long value = strtol(token, NULL, 10);
//         reply[csv->map[i]] = enif_make_long(env, value);
//       } else if(csv->filters[i] == FLOAT_FILTER) {
//         double value = strtod(token, NULL);
//         reply[csv->map[i]] = enif_make_double(env, value);
//       } else if(csv->filters[i] == CENTS_FILTER) {
//         char value[100];
//         strncpy(value, token, sizeof(value));
//         char *p = value, *tok;
//         long dollars = 0;
//         long cents = 0;
//         tok = strsep(&p, ".");
//         dollars = strtol(tok, NULL, 10);
//         tok = strsep(&p, ".");
//         if(tok && strlen(tok) > 0) {
//           if(strlen(tok) > 2) tok[2] = 0;
//           cents = strtol(tok, NULL, 10);
//           if(strlen(tok) == 1) cents = cents*10;
//         }
//         reply[csv->map[i]] = enif_make_long(env, dollars*100 + cents);
//       } else if(csv->filters[i] == GMT_OFFSET_FILTER) {
//         int offt = 0;
//         sscanf(token, "%d", &offt);
//         reply[csv->map[i]] = enif_make_int(env, offt);
//       } else if(csv->filters[i] == DATE_FILTER) {
//         int y = 0,m = 0,d = 0;
//         sscanf(token, "%4d%2d%2d", &y, &m, &d);
//         reply[csv->map[i]] = enif_make_tuple3(env,
//           enif_make_int(env, y),
//           enif_make_int(env, m),
//           enif_make_int(env, d)
//         );
// 
//         if(csv->filters_addons[i] != -1) {
//           int h = 0,m = 0,s = 0,ms = 0;
//           sscanf(tokens[csv->filters_addons[i]], "%2d:%2d:%2d.%3d", &h, &m, &s, &ms);
//           
//           reply[csv->map[i]] = enif_make_tuple2(env, reply[csv->map[i]],
//             enif_make_tuple4(env,
//             enif_make_int(env, h),
//             enif_make_int(env, m),
//             enif_make_int(env, s),
//             enif_make_int(env, ms)
//             )
//           );
//           
//         }
//       } else if(csv->filters[i] == TIME_FILTER && csv->filters_addons[i]) {
//         
//         int y = 0,m = 0,d = 0;
//         sscanf(tokens[csv->filters_addons[i]], "%4d%2d%2d", &y, &m, &d);
//         
//         int h = 0,min = 0,s = 0,ms = 0;
//         sscanf(token, "%2d:%2d:%2d.%3d", &h, &min, &s, &ms);
//         
//         char buf[1024];
//         snprintf(buf, sizeof(buf), "%4d/%02d/%02d %02d:%02d:%02d.%03d", y, m, d, h, min, s, ms);
//         
//         ErlNifBinary bin;
//         enif_alloc_binary(strlen(buf), &bin);
//         memcpy(bin.data, buf, strlen(buf));
//         
//         reply[csv->map[i]] = enif_make_binary(env, &bin);
//       } else {
//         ErlNifBinary bin;
//         enif_alloc_binary(strlen(token), &bin);
//         memcpy(bin.data, token, strlen(token));
//         reply[csv->map[i]] = enif_make_binary(env, &bin);
//       }
//     }    
//   }
//   
//   return enif_make_tuple_from_array(env, reply, csv->out_size);
// }
// 
// static ERL_NIF_TERM
// csv_next_batch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
// {
//   int count;
//   if(!enif_get_int(env, argv[1], &count)) return enif_make_badarg(env);
// 
//   CSV *csv;
//   
//   if(!enif_get_resource(env, argv[0], CSVResource, (void **)&csv)) {
//     return enif_make_badarg(env);
//   }
//   
//   ERL_NIF_TERM reply[count];
//   int i;
//   for(i = 0; i < count; i++) {
//     ERL_NIF_TERM r = csv_next(env, 1, argv);
//     if(enif_is_atom(env, r) && i == 0) return r;
//     if(enif_is_atom(env, r)) break;
//     reply[i] = r;
//   }
//   count = i;
//   return enif_make_list_from_array(env, reply, count);
// }
// 
// static ERL_NIF_TERM
// date_to_ms(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
// {
//   struct tm tp;
//   bzero(&tp, sizeof(tp));
//   int ms = 0;
//   
//   if(!enif_get_int(env, argv[0], &tp.tm_year)) return enif_make_badarg(env);
//   if(!enif_get_int(env, argv[1], &tp.tm_mon)) return enif_make_badarg(env);
//   if(!enif_get_int(env, argv[2], &tp.tm_mday)) return enif_make_badarg(env);
//   if(!enif_get_int(env, argv[3], &tp.tm_hour)) return enif_make_badarg(env);
//   if(!enif_get_int(env, argv[4], &tp.tm_min)) return enif_make_badarg(env);
//   if(!enif_get_int(env, argv[5], &tp.tm_sec)) return enif_make_badarg(env);
//   if(!enif_get_int(env, argv[6], &ms)) return enif_make_badarg(env);
//   
//   tp.tm_year = tp.tm_year - 1900;
//   tp.tm_mon--;
//   time_t t = timegm(&tp);
//   // fprintf(stderr, "UTC: %d/%d/%d %d:%d:%d.%d %lu\r\n", tp.tm_year, tp.tm_mon, tp.tm_mday,
//   // tp.tm_hour, tp.tm_min, tp.tm_sec, ms, (long unsigned)t);
//   return enif_make_uint64(env, (ErlNifUInt64)(t*1000 + ms));
// }


typedef struct Pattern {
  // ERL_NIF_TERM header
} Pattern;

static ERL_NIF_TERM
parse_line(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary bin, pattern;
  if(!enif_inspect_binary(env, argv[0], &bin)) return enif_make_badarg(env);
  if(!enif_inspect_binary(env, argv[1], &pattern)) return enif_make_badarg(env);
  
  int i;
  for(i = 0; i < bin.size; i++) {
    if(bin.data[i] == '\n') {
      ERL_NIF_TERM line = enif_make_sub_binary(env, argv[0], 0, i);
      ERL_NIF_TERM rest = enif_make_sub_binary(env, argv[0], i+1, bin.size - i - 1);
      return enif_make_tuple2(env, line, rest);
    }
  }
  return enif_make_tuple2(env, enif_make_atom(env, "undefined"), argv[0]);
}

static ErlNifFunc csv_funcs[] =
{
  // {"csv_open", 2, csv_open},
  // {"csv_next", 1, csv_next},
  // {"csv_next_batch", 2, csv_next_batch},
  // {"date_to_ms_nif", 7, date_to_ms}
  {"parse_line", 2, parse_line}
};


ERL_NIF_INIT(csv_reader, csv_funcs, load, NULL, NULL, NULL)
