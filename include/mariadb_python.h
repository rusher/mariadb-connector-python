/************************************************************************************
    Copyright (C) 2018 Georg Richter and MariaDB Corporation AB

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc.,
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
*************************************************************************************/
#include "Python.h"
#include "bytesobject.h"
#include "structmember.h"
#include "structseq.h"
#include <stdarg.h>
#include <mysql.h>
#include <errmsg.h>
#include <mysqld_error.h>
#include <time.h>
#include <malloc.h>
#include <docs/common.h>

#if defined(_WIN32) && defined(_MSVC)
#ifndef L64
#define L64(x) x##i64
#endif
#else
#ifndef L64
#define L64(x) x##LL
#endif /* L64 */
#endif /* _WIN32 */

#define MAX_TPC_XID_SIZE 65

/* Magic constant for checking dynamic columns */
#define PYTHON_DYNCOL_VALUE 0xA378BD8E

enum enum_dataapi_groups
{
  DBAPI_NUMBER= 1,
  DBAPI_STRING,
  DBAPI_DATETIME,
  DBAPI_BINARY,
  DBAPI_ROWID
};

enum enum_dyncol_type
{
  DYNCOL_LIST= 1,
  DYNCOL_TUPLE,
  DYNCOL_SET,
  DYNCOL_DICT,
  DYNCOL_ODICT,
  DYNCOL_LAST
};

enum enum_tpc_state
{
  TPC_STATE_NONE= 0,
  TPC_STATE_XID,
  TPC_STATE_PREPARE
};

/* PEP-249: Connection object */
typedef struct {
	PyObject_HEAD
	MYSQL *mysql;
	int open;
  uint8_t is_buffered;
  uint8_t is_closed;
  enum enum_tpc_state tpc_state;
  char xid[MAX_TPC_XID_SIZE];
  PyObject *dsn; /* always null */
  PyObject *tls_cipher;
  PyObject *tls_version;
  PyObject *host;
  PyObject *unix_socket;
  int port;
  PyObject *charset;
  PyObject *collation;
} MrdbConnection;

typedef struct {
  enum enum_field_types type;
  PyObject *Value;
  char indicator;
} Mariadb_Value;

/* Parameter info for cursor.executemany()
   operations */
typedef struct {
  enum enum_field_types type;
  size_t bits; /* for PyLong Object */
  PyTypeObject *ob_type;
  uint8_t is_negative;
  uint8_t has_indicator;
} MrdbParamInfo;

typedef struct {
  PyObject *value;
  char indicator;
  enum enum_field_types type;
  size_t length;
  uint8_t free_me;
  void *buffer;
  unsigned char num[8];
  MYSQL_TIME tm;
} MrdbParamValue;

typedef struct {
  PyObject_HEAD
  enum enum_indicator_type indicator;
} MrdbIndicator;

/* PEP-249: Cursor object */
typedef struct {
  PyObject_HEAD
  MrdbConnection *connection;
  MYSQL_STMT *stmt;
  MYSQL_RES *result;
  PyObject *data;
  uint32_t array_size;
  uint32_t param_count;
  uint32_t row_array_size; /* for fetch many */
  MrdbParamInfo *paraminfo;
  MrdbParamValue *value;
  MYSQL_BIND *params;
  MYSQL_BIND *bind;
  MYSQL_FIELD *fields;
  char *statement;
  unsigned long statement_len;
  PyObject **values;
  PyStructSequence_Desc sequence_desc;
  PyStructSequence_Field *sequence_fields;
  PyTypeObject *sequence_type;
  unsigned long prefetch_rows;
  unsigned long cursor_type;
  int64_t affected_rows;
  int64_t row_count;
  unsigned long row_number;
  uint8_t is_prepared;
  uint8_t is_buffered;
  uint8_t is_named_tuple;
  uint8_t is_closed;
  uint8_t is_text;
} MrdbCursor;

typedef struct
{
  PyObject_HEAD
} Mariadb_Fieldinfo;

typedef struct
{
  PyObject_HEAD
  int32_t *types;
} Mariadb_DBAPIType;

typedef struct {
  ps_field_fetch_func func;
  int pack_len;
  unsigned long max_len;
} Mariadb_Conversion;


/* Exceptions */
PyObject *Mariadb_InterfaceError;
PyObject *Mariadb_Error;
PyObject *Mariadb_DatabaseError;
PyObject *Mariadb_DataError;
PyObject *Mariadb_OperationalError;
PyObject *Mariadb_IntegrityError;
PyObject *Mariadb_InternalError;
PyObject *Mariadb_ProgrammingError;
PyObject *Mariadb_NotSupportedError;
PyObject *Mariadb_Warning;

PyObject *Mrdb_Pickle;

/* Object types */
PyTypeObject Mariadb_Fieldinfo_Type;
PyTypeObject MrdbIndicator_Type;
PyTypeObject MrdbConnection_Type;
PyTypeObject MrdbCursor_Type;
PyTypeObject Mariadb_DBAPIType_Type;

int Mariadb_traverse(PyObject *self,
                     visitproc visit,
                     void *arg);

/* Function prototypes */
void mariadb_throw_exception(void *handle,
                             PyObject *execption_type,
                             unsigned char is_statement,
                             const char *message,
                             ...);

PyObject *MrdbIndicator_Object(uint32_t type);
long MrdbIndicator_AsLong(PyObject *v);
PyObject *Mariadb_DBAPIType_Object(uint32_t type);
PyObject *MrdbConnection_affected_rows(MrdbConnection *self);
PyObject *MrdbConnection_ping(MrdbConnection *self, PyObject *args);
PyObject *MrdbConnection_kill(MrdbConnection *self, PyObject *args);
PyObject *MrdbConnection_reconnect(MrdbConnection *self);
PyObject *MrdbConnection_reset(MrdbConnection *self);
PyObject *MrdbConnection_autocommit(MrdbConnection *self,
                             PyObject *args);
PyObject *MrdbConnection_change_user(MrdbConnection *self,
                                     PyObject *args);
PyObject *MrdbConnection_rollback(MrdbConnection *self);
PyObject *MrdbConnection_commit(MrdbConnection *self);
PyObject *MrdbConnection_close(MrdbConnection *self);
PyObject *MrdbConnection_connect( PyObject *self,PyObject *args,	PyObject *kwargs);
void MrdbConnection_SetAttributes(MrdbConnection *self);

/* TPC methods */
PyObject *MrdbConnection_xid(MrdbConnection *self, PyObject *args);
PyObject *MrdbConnection_tpc_begin(MrdbConnection *self, PyObject *args);
PyObject *MrdbConnection_tpc_commit(MrdbConnection *self, PyObject *args);
PyObject *MrdbConnection_tpc_rollback(MrdbConnection *self, PyObject *args);
PyObject *MrdbConnection_tpc_prepare(MrdbConnection *self);
PyObject *MrdbConnection_tpc_recover(MrdbConnection *self);

/* codecs prototypes  */
uint8_t mariadb_check_bulk_parameters(MrdbCursor *self,
                                      PyObject *data);
uint8_t mariadb_check_execute_parameters(MrdbCursor *self,
                                      PyObject *data);
uint8_t mariadb_param_update(void *data, MYSQL_BIND *bind, uint32_t row_nr);
/* Global defines */


#define MARIADB_PY_APILEVEL "2.0"
#define MARIADB_PY_PARAMSTYLE "qmark"
#define MARIADB_PY_THREADSAFETY 1


/* Helper macros */

#define MrdbIndicator_Check(a)\
(Py_TYPE((a)) == &MrdbIndicator_Type)

#define MARIADB_FEATURE_SUPPORTED(mysql,version)\
(mysql_get_server_version((mysql)) >= (version))

#define MARIADB_CHECK_CONNECTION(connection, ret)\
if (!connection || !connection->mysql) {\
  mariadb_throw_exception(connection->mysql, Mariadb_Error, 0,\
    "Invalid connection or not connected");\
    return (ret);\
}

#define MARIADB_CHECK_TPC(connection)\
if (connection->tpc_state == TPC_STATE_NONE) {\
  mariadb_throw_exception(connection->mysql, Mariadb_ProgrammingError, 0,\
    "Transaction not started");\
    return NULL;\
}

#define MARIADB_FREE_MEM(a)\
if (a) {\
  PyMem_RawFree((a));\
  (a)= NULL;\
}

#define MARIADB_CHECK_STMT(cursor)\
if (!cursor->stmt || !cursor->stmt->mysql || cursor->is_closed)\
{\
  (cursor)->is_closed= 1;\
  mariadb_throw_exception(cursor->stmt, Mariadb_ProgrammingError, 1,\
    "Invalid cursor or not connected");\
}

/* MariaDB protocol macros */
#define int1store(T,A) *((int8_t*) (T)) = (A)
#define uint1korr(A)   (*(((uint8_t*)(A))))
#if defined(__i386__) || defined(_WIN32)
#define sint2korr(A)	(*((int16_t *) (A)))
#define sint3korr(A)	((int32_t) ((((unsigned char) (A)[2]) & 128) ? \
				  (((uint32_t) 255L << 24) | \
				   (((uint32_t) (unsigned char) (A)[2]) << 16) |\
				   (((uint32_t) (unsigned char) (A)[1]) << 8) | \
				   ((uint32_t) (unsigned char) (A)[0])) : \
				  (((uint32_t) (unsigned char) (A)[2]) << 16) |\
				  (((uint32_t) (unsigned char) (A)[1]) << 8) | \
				  ((uint32_t) (unsigned char) (A)[0])))
#define sint4korr(A)	(*((long *) (A)))
#define uint2korr(A)	(*((uint16_t *) (A)))
#if defined(HAVE_purify) && !defined(_WIN32)
#define uint3korr(A)	(uint32_t) (((uint32_t) ((unsigned char) (A)[0])) +\
				  (((uint32_t) ((unsigned char) (A)[1])) << 8) +\
				  (((uint32_t) ((unsigned char) (A)[2])) << 16))
#else
/*
   ATTENTION !
   
    Please, note, uint3korr reads 4 bytes (not 3) !
    It means, that you have to provide enough allocated space !
*/
#define uint3korr(A)	(long) (*((unsigned int *) (A)) & 0xFFFFFF)
#endif /* HAVE_purify && !_WIN32 */
#define uint4korr(A)	(*((uint32_t *) (A)))
#define uint5korr(A)	((unsigned long long)(((uint32_t) ((unsigned char) (A)[0])) +\
				    (((uint32_t) ((unsigned char) (A)[1])) << 8) +\
				    (((uint32_t) ((unsigned char) (A)[2])) << 16) +\
				    (((uint32_t) ((unsigned char) (A)[3])) << 24)) +\
				    (((unsigned long long) ((unsigned char) (A)[4])) << 32))
#define uint6korr(A)	((unsigned long long)(((uint32_t)    ((unsigned char) (A)[0]))          + \
                                     (((uint32_t)    ((unsigned char) (A)[1])) << 8)   + \
                                     (((uint32_t)    ((unsigned char) (A)[2])) << 16)  + \
                                     (((uint32_t)    ((unsigned char) (A)[3])) << 24)) + \
                         (((unsigned long long) ((unsigned char) (A)[4])) << 32) +       \
                         (((unsigned long long) ((unsigned char) (A)[5])) << 40))
#define uint8_tkorr(A)	(*((unsigned long long *) (A)))
#define sint8korr(A)	(*((long long *) (A)))
#define int2store(T,A)	*((uint16_t*) (T))= (uint16_t) (A)
#define int3store(T,A)  do { *(T)=  (unsigned char) ((A));\
                            *(T+1)=(unsigned char) (((uint) (A) >> 8));\
                            *(T+2)=(unsigned char) (((A) >> 16)); } while (0)
#define int4store(T,A)	*((long *) (T))= (long) (A)
#define int5store(T,A)  do { *(T)= (unsigned char)((A));\
                             *((T)+1)=(unsigned char) (((A) >> 8));\
                             *((T)+2)=(unsigned char) (((A) >> 16));\
                             *((T)+3)=(unsigned char) (((A) >> 24)); \
                             *((T)+4)=(unsigned char) (((A) >> 32)); } while(0)
#define int6store(T,A)  do { *(T)=    (unsigned char)((A));          \
                             *((T)+1)=(unsigned char) (((A) >> 8));  \
                             *((T)+2)=(unsigned char) (((A) >> 16)); \
                             *((T)+3)=(unsigned char) (((A) >> 24)); \
                             *((T)+4)=(unsigned char) (((A) >> 32)); \
                             *((T)+5)=(unsigned char) (((A) >> 40)); } while(0)
#define int8store(T,A)	*((unsigned long long *) (T))= (unsigned long long) (A)

typedef union {
  double v;
  long m[2];
} doubleget_union;
#define doubleget(V,M)	\
do { doubleget_union _tmp; \
     _tmp.m[0] = *((long*)(M)); \
     _tmp.m[1] = *(((long*) (M))+1); \
     (V) = _tmp.v; } while(0)
#define doublestore(T,V) do { *((long *) T) = ((doubleget_union *)&V)->m[0]; \
			     *(((long *) T)+1) = ((doubleget_union *)&V)->m[1]; \
                         } while (0)
#define float4get(V,M)   do { *((float *) &(V)) = *((float*) (M)); } while(0)
#define float8get(V,M)   doubleget((V),(M))
#define float4store(V,M) memcpy((unsigned char*) V,(unsigned char*) (&M),sizeof(float))
#define floatstore(T,V)  memcpy((unsigned char*)(T), (unsigned char*)(&V),sizeof(float))
#define floatget(V,M)    memcpy((unsigned char*) &V,(unsigned char*) (M),sizeof(float))
#define float8store(V,M) doublestore((V),(M))
#else

/*
  We're here if it's not a IA-32 architecture (Win32 and UNIX IA-32 defines
  were done before)
*/
#define sint2korr(A)	(int16_t) (((int16_t) ((unsigned char) (A)[0])) +\
				 ((int16_t) ((int16_t) (A)[1]) << 8))
#define sint3korr(A)	((int32_t) ((((unsigned char) (A)[2]) & 128) ? \
				  (((uint32_t) 255L << 24) | \
				   (((uint32_t) (unsigned char) (A)[2]) << 16) |\
				   (((uint32_t) (unsigned char) (A)[1]) << 8) | \
				   ((uint32_t) (unsigned char) (A)[0])) : \
				  (((uint32_t) (unsigned char) (A)[2]) << 16) |\
				  (((uint32_t) (unsigned char) (A)[1]) << 8) | \
				  ((uint32_t) (unsigned char) (A)[0])))
#define sint4korr(A)	(int32_t) (((int32_t) ((unsigned char) (A)[0])) +\
				(((int32_t) ((unsigned char) (A)[1]) << 8)) +\
				(((int32_t) ((unsigned char) (A)[2]) << 16)) +\
				(((int32_t) ((int16_t) (A)[3]) << 24)))
#define sint8korr(A)	(long long) uint8korr(A)
#define uint2korr(A)	(uint16_t) (((uint16_t) ((unsigned char) (A)[0])) +\
				  ((uint16_t) ((unsigned char) (A)[1]) << 8))
#define uint3korr(A)	(uint32_t) (((uint32_t) ((unsigned char) (A)[0])) +\
				  (((uint32_t) ((unsigned char) (A)[1])) << 8) +\
				  (((uint32_t) ((unsigned char) (A)[2])) << 16))
#define uint4korr(A)	(uint32_t) (((uint32_t) ((unsigned char) (A)[0])) +\
				  (((uint32_t) ((unsigned char) (A)[1])) << 8) +\
				  (((uint32_t) ((unsigned char) (A)[2])) << 16) +\
				  (((uint32_t) ((unsigned char) (A)[3])) << 24))
#define uint5korr(A)	((unsigned long long)(((uint32_t) ((unsigned char) (A)[0])) +\
				    (((uint32_t) ((unsigned char) (A)[1])) << 8) +\
				    (((uint32_t) ((unsigned char) (A)[2])) << 16) +\
				    (((uint32_t) ((unsigned char) (A)[3])) << 24)) +\
				    (((unsigned long long) ((unsigned char) (A)[4])) << 32))
#define uint6korr(A)	((unsigned long long)(((uint32_t)    ((unsigned char) (A)[0]))          + \
                                     (((uint32_t)    ((unsigned char) (A)[1])) << 8)   + \
                                     (((uint32_t)    ((unsigned char) (A)[2])) << 16)  + \
                                     (((uint32_t)    ((unsigned char) (A)[3])) << 24)) + \
                         (((unsigned long long) ((unsigned char) (A)[4])) << 32) +       \
                         (((unsigned long long) ((unsigned char) (A)[5])) << 40))
#define uint8korr(A)	((unsigned long long)(((uint32_t) ((unsigned char) (A)[0])) +\
				    (((uint32_t) ((unsigned char) (A)[1])) << 8) +\
				    (((uint32_t) ((unsigned char) (A)[2])) << 16) +\
				    (((uint32_t) ((unsigned char) (A)[3])) << 24)) +\
			(((unsigned long long) (((uint32_t) ((unsigned char) (A)[4])) +\
				    (((uint32_t) ((unsigned char) (A)[5])) << 8) +\
				    (((uint32_t) ((unsigned char) (A)[6])) << 16) +\
				    (((uint32_t) ((unsigned char) (A)[7])) << 24))) <<\
				    32))
#define int2store(T,A)       do { uint def_temp= (uint) (A) ;\
                                  *((unsigned char*) (T))=  (unsigned char)(def_temp); \
                                   *((unsigned char*) (T)+1)=(unsigned char)((def_temp >> 8)); \
                             } while(0)
#define int3store(T,A)       do { /*lint -save -e734 */\
                                  *((unsigned char*)(T))=(unsigned char) ((A));\
                                  *((unsigned char*) (T)+1)=(unsigned char) (((A) >> 8));\
                                  *((unsigned char*)(T)+2)=(unsigned char) (((A) >> 16)); \
                                  /*lint -restore */} while(0)
#define int4store(T,A)       do { *((char *)(T))=(char) ((A));\
                                  *(((char *)(T))+1)=(char) (((A) >> 8));\
                                  *(((char *)(T))+2)=(char) (((A) >> 16));\
                                  *(((char *)(T))+3)=(char) (((A) >> 24)); } while(0)
#define int5store(T,A)       do { *((char *)(T))=     (char)((A));  \
                                  *(((char *)(T))+1)= (char)(((A) >> 8)); \
                                  *(((char *)(T))+2)= (char)(((A) >> 16)); \
                                  *(((char *)(T))+3)= (char)(((A) >> 24)); \
                                  *(((char *)(T))+4)= (char)(((A) >> 32)); \
		                } while(0)
#define int6store(T,A)       do { *((char *)(T))=     (char)((A)); \
                                  *(((char *)(T))+1)= (char)(((A) >> 8)); \
                                  *(((char *)(T))+2)= (char)(((A) >> 16)); \
                                  *(((char *)(T))+3)= (char)(((A) >> 24)); \
                                  *(((char *)(T))+4)= (char)(((A) >> 32)); \
                                  *(((char *)(T))+5)= (char)(((A) >> 40)); \
                                } while(0)
#define int8store(T,A)       do { uint def_temp= (uint) (A), def_temp2= (uint) ((A) >> 32); \
                                  int4store((T),def_temp); \
                                  int4store((T+4),def_temp2); } while(0)
#ifdef WORDS_BIGENDIAN
#define float4store(T,A) do { *(T)= ((unsigned char *) &A)[3];\
                              *((T)+1)=(char) ((unsigned char *) &A)[2];\
                              *((T)+2)=(char) ((unsigned char *) &A)[1];\
                              *((T)+3)=(char) ((unsigned char *) &A)[0]; } while(0)

#define float4get(V,M)   do { float def_temp;\
                              ((unsigned char*) &def_temp)[0]=(M)[3];\
                              ((unsigned char*) &def_temp)[1]=(M)[2];\
                              ((unsigned char*) &def_temp)[2]=(M)[1];\
                              ((unsigned char*) &def_temp)[3]=(M)[0];\
                              (V)=def_temp; } while(0)
#define float8store(T,V) do { *(T)= ((unsigned char *) &V)[7];\
                              *((T)+1)=(char) ((unsigned char *) &V)[6];\
                              *((T)+2)=(char) ((unsigned char *) &V)[5];\
                              *((T)+3)=(char) ((unsigned char *) &V)[4];\
                              *((T)+4)=(char) ((unsigned char *) &V)[3];\
                              *((T)+5)=(char) ((unsigned char *) &V)[2];\
                              *((T)+6)=(char) ((unsigned char *) &V)[1];\
                              *((T)+7)=(char) ((unsigned char *) &V)[0]; } while(0)

#define float8get(V,M)   do { double def_temp;\
                              ((unsigned char*) &def_temp)[0]=(M)[7];\
                              ((unsigned char*) &def_temp)[1]=(M)[6];\
                              ((unsigned char*) &def_temp)[2]=(M)[5];\
                              ((unsigned char*) &def_temp)[3]=(M)[4];\
                              ((unsigned char*) &def_temp)[4]=(M)[3];\
                              ((unsigned char*) &def_temp)[5]=(M)[2];\
                              ((unsigned char*) &def_temp)[6]=(M)[1];\
                              ((unsigned char*) &def_temp)[7]=(M)[0];\
                              (V) = def_temp; } while(0)
#else
#define float4get(V,M)   memcpy(&V, (M), sizeof(float))
#define float4store(V,M) memcpy(V, (&M), sizeof(float))

#if defined(__FLOAT_WORD_ORDER) && (__FLOAT_WORD_ORDER == __BIG_ENDIAN)
#define doublestore(T,V) do { *(((char*)T)+0)=(char) ((unsigned char *) &V)[4];\
                              *(((char*)T)+1)=(char) ((unsigned char *) &V)[5];\
                              *(((char*)T)+2)=(char) ((unsigned char *) &V)[6];\
                              *(((char*)T)+3)=(char) ((unsigned char *) &V)[7];\
                              *(((char*)T)+4)=(char) ((unsigned char *) &V)[0];\
                              *(((char*)T)+5)=(char) ((unsigned char *) &V)[1];\
                              *(((char*)T)+6)=(char) ((unsigned char *) &V)[2];\
                              *(((char*)T)+7)=(char) ((unsigned char *) &V)[3]; }\
                         while(0)
#define doubleget(V,M)   do { double def_temp;\
                              ((unsigned char*) &def_temp)[0]=(M)[4];\
                              ((unsigned char*) &def_temp)[1]=(M)[5];\
                              ((unsigned char*) &def_temp)[2]=(M)[6];\
                              ((unsigned char*) &def_temp)[3]=(M)[7];\
                              ((unsigned char*) &def_temp)[4]=(M)[0];\
                              ((unsigned char*) &def_temp)[5]=(M)[1];\
                              ((unsigned char*) &def_temp)[6]=(M)[2];\
                              ((unsigned char*) &def_temp)[7]=(M)[3];\
                              (V) = def_temp; } while(0)
#endif /* __FLOAT_WORD_ORDER */

#define float8get(V,M)   doubleget((V),(M))
#define float8store(V,M) doublestore((V),(M))
#endif /* WORDS_BIGENDIAN */

#ifdef HAVE_BIGENDIAN

#define ushortget(V,M)  do { V = (uint16_t) (((uint16_t) ((unsigned char) (M)[1]))+\
                                 ((uint16_t) ((uint16_t) (M)[0]) << 8)); } while(0)
#define shortget(V,M)   do { V = (short) (((short) ((unsigned char) (M)[1]))+\
                                 ((short) ((short) (M)[0]) << 8)); } while(0)
#define longget(V,M)    do { int32 def_temp;\
                             ((unsigned char*) &def_temp)[0]=(M)[0];\
                             ((unsigned char*) &def_temp)[1]=(M)[1];\
                             ((unsigned char*) &def_temp)[2]=(M)[2];\
                             ((unsigned char*) &def_temp)[3]=(M)[3];\
                             (V)=def_temp; } while(0)
#define ulongget(V,M)   do { uint32 def_temp;\
                            ((unsigned char*) &def_temp)[0]=(M)[0];\
                            ((unsigned char*) &def_temp)[1]=(M)[1];\
                            ((unsigned char*) &def_temp)[2]=(M)[2];\
                            ((unsigned char*) &def_temp)[3]=(M)[3];\
                            (V)=def_temp; } while(0)
#define shortstore(T,A) do { uint def_temp=(uint) (A) ;\
                             *(((char*)T)+1)=(char)(def_temp); \
                             *(((char*)T)+0)=(char)(def_temp >> 8); } while(0)
#define longstore(T,A)  do { *(((char*)T)+3)=((A));\
                             *(((char*)T)+2)=(((A) >> 8));\
                             *(((char*)T)+1)=(((A) >> 16));\
                             *(((char*)T)+0)=(((A) >> 24)); } while(0)

#define floatget(V,M)    memcpy(&V, (M), sizeof(float))
#define floatstore(T,V)  memcpy((T), (void*) (&V), sizeof(float))
#define doubleget(V,M)	 memcpy(&V, (M), sizeof(double))
#define doublestore(T,V) memcpy((T), (void *) &V, sizeof(double))
#define longlongget(V,M) memcpy(&V, (M), sizeof(unsigned long long))
#define longlongstore(T,V) memcpy((T), &V, sizeof(unsigned long long))

#else

#define ushortget(V,M)	do { V = uint2korr(M); } while(0)
#define shortget(V,M)	do { V = sint2korr(M); } while(0)
#define longget(V,M)	do { V = sint4korr(M); } while(0)
#define ulongget(V,M)   do { V = uint4korr(M); } while(0)
#define shortstore(T,V) int2store(T,V)
#define longstore(T,V)	int4store(T,V)
#ifndef floatstore
#define floatstore(T,V)  memcpy((T), (void *) (&V), sizeof(float))
#define floatget(V,M)    memcpy(&V, (M), sizeof(float))
#endif
#ifndef doubleget
#define doubleget(V,M)	 memcpy(&V, (M), sizeof(double))
#define doublestore(T,V) memcpy((T), (void *) &V, sizeof(double))
#endif /* doubleget */
#define longlongget(V,M) memcpy(&V, (M), sizeof(unsigned long long))
#define longlongstore(T,V) memcpy((T), &V, sizeof(unsigned long long))

#endif /* WORDS_BIGENDIAN */


#endif /* __i386__ OR _WIN32 */

#ifdef _WIN32
//#define alloca _malloca
#endif
