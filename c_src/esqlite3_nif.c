/*
 * Copyright 2011 - 2022 Maas-Maarten Zeeman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

/*
 * sqlite3_nif -- an erlang sqlite nif.
*/

#include <erl_nif.h>
#include <string.h>
#include <stdio.h>

#include <sqlite3.h>

#define MAX_ATOM_LENGTH 255         /* from atom.h, not exposed in erlang include */
#define MAX_SQLITE_NAME_LENGTH  255 /* Maximum name length. Using longer will return misuse */
#define MAX_PATHNAME 512            /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite3_type = NULL;
static ErlNifResourceType *esqlite3_stmt_type = NULL;
static ErlNifResourceType *esqlite3_backup_type = NULL;

/* database connection context */
typedef struct {
    sqlite3 *db;

    ErlNifPid *update_hook_pid;
} esqlite3;

/* prepared statement */
typedef struct {
    sqlite3_stmt *statement;

    int column_count;
} esqlite3_stmt;

/* data associated with ongoing backup */
typedef struct {
    sqlite3_backup *backup;
} esqlite3_backup;

static ERL_NIF_TERM atom_esqlite3;

static ERL_NIF_TERM
make_atom(ErlNifEnv *env, const char *atom_name)
{
    ERL_NIF_TERM atom;

    if(enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1))
        return atom;

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM
make_ok_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
{
    return enif_make_tuple2(env, make_atom(env, "ok"), value);
}

static ERL_NIF_TERM
make_error_tuple(ErlNifEnv *env, const char *reason)
{
    return enif_make_tuple2(env, make_atom(env, "error"), make_atom(env, reason));
}

static ERL_NIF_TERM
make_row_tuple(ErlNifEnv *env, ERL_NIF_TERM value)
{
    return enif_make_tuple2(env, make_atom(env, "row"), value);
}

static const char *
get_sqlite3_return_code_msg(int r)
{
    switch(r) {
        case SQLITE_OK: return "ok";
        case SQLITE_ERROR : return "sqlite_error";
        case SQLITE_INTERNAL: return "internal";
        case SQLITE_PERM: return "perm";
        case SQLITE_ABORT: return "abort";
        case SQLITE_BUSY: return "$busy";
        case SQLITE_LOCKED: return  "locked";
        case SQLITE_NOMEM: return  "nomem";
        case SQLITE_READONLY: return  "readonly";
        case SQLITE_INTERRUPT: return  "interrupt";
        case SQLITE_IOERR: return  "ioerror";
        case SQLITE_CORRUPT: return  "corrupt";
        case SQLITE_NOTFOUND: return  "notfound";
        case SQLITE_FULL: return  "full";
        case SQLITE_CANTOPEN: return  "cantopen";
        case SQLITE_PROTOCOL: return  "protocol";
        case SQLITE_EMPTY: return  "empty";
        case SQLITE_SCHEMA: return  "schema";
        case SQLITE_TOOBIG: return  "toobig";
        case SQLITE_CONSTRAINT: return  "constraint";
        case SQLITE_MISMATCH: return  "mismatch";
        case SQLITE_MISUSE: return  "misuse";
        case SQLITE_NOLFS: return  "nolfs";
        case SQLITE_AUTH: return  "auth";
        case SQLITE_FORMAT: return  "format";
        case SQLITE_RANGE: return  "range";
        case SQLITE_NOTADB: return  "notadb";
        case SQLITE_ROW: return  "row";
        case SQLITE_DONE: return  "$done";
    }
    
    return  "unknown";
}

ERL_NIF_TERM
make_two_atom_tuple(ErlNifEnv *env, const char *one, const char *two) {
    return enif_make_tuple2(env, make_atom(env, one), make_atom(env, two));
}

ERL_NIF_TERM
make_extended_error_tuple(ErlNifEnv *env, int code) {
    switch(code) {
        case SQLITE_MISUSE:
            return make_two_atom_tuple(env, "misuse", "invoked_incorrectly");
        case SQLITE_ERROR_MISSING_COLLSEQ:
            return make_two_atom_tuple(env, "error", "missing_collesq");
        case SQLITE_ERROR_RETRY:
            return make_two_atom_tuple(env, "error", "retry");
        case SQLITE_ERROR_SNAPSHOT:
            return make_two_atom_tuple(env, "error", "snapshot");
        case SQLITE_IOERR_READ:
            return make_two_atom_tuple(env, "ioerr", "read");
        case SQLITE_IOERR_SHORT_READ:
            return make_two_atom_tuple(env, "ioerr", "short_read");
        case SQLITE_IOERR_WRITE:
            return make_two_atom_tuple(env, "ioerr", "write");
        case SQLITE_IOERR_FSYNC:
            return make_two_atom_tuple(env, "ioerr", "fsync");
        case SQLITE_IOERR_DIR_FSYNC:
            return make_two_atom_tuple(env, "ioerr", "dir_fsync");
        case SQLITE_IOERR_TRUNCATE:
            return make_two_atom_tuple(env, "ioerr", "truncate");
        case SQLITE_IOERR_FSTAT:
            return make_two_atom_tuple(env, "ioerr", "fstat");
        case SQLITE_IOERR_UNLOCK:
            return make_two_atom_tuple(env, "ioerr", "unlock");
        case SQLITE_IOERR_RDLOCK:
            return make_two_atom_tuple(env, "ioerr", "rdlock");
        case SQLITE_IOERR_DELETE:
            return make_two_atom_tuple(env, "ioerr", "delete");
        case SQLITE_IOERR_BLOCKED:
            return make_two_atom_tuple(env, "ioerr", "blocked");
        case SQLITE_IOERR_NOMEM:
            return make_two_atom_tuple(env, "ioerr", "nomem");
        case SQLITE_IOERR_ACCESS:
            return make_two_atom_tuple(env, "ioerr", "access");
        case SQLITE_IOERR_CHECKRESERVEDLOCK:
            return make_two_atom_tuple(env, "ioerr", "checkreservedlock");
        case SQLITE_IOERR_LOCK:
            return make_two_atom_tuple(env, "ioerr", "lock");
        case SQLITE_IOERR_CLOSE:
            return make_two_atom_tuple(env, "ioerr", "close");
        case SQLITE_IOERR_DIR_CLOSE:
            return make_two_atom_tuple(env, "ioerr", "dir_close");
        case SQLITE_IOERR_SHMOPEN:
            return make_two_atom_tuple(env, "ioerr", "shmopen"); 
        case SQLITE_IOERR_SHMSIZE:
            return make_two_atom_tuple(env, "ioerr", "shmsize"); 
        case SQLITE_IOERR_SHMLOCK:
            return make_two_atom_tuple(env, "ioerr", "shmlock");
        case SQLITE_IOERR_SHMMAP:
            return make_two_atom_tuple(env, "ioerr", "shmmap");
        case SQLITE_IOERR_SEEK:
            return make_two_atom_tuple(env, "ioerr", "seek");
        case SQLITE_IOERR_DELETE_NOENT:
            return make_two_atom_tuple(env, "ioerr", "delete_noent");
        case SQLITE_IOERR_MMAP:
            return make_two_atom_tuple(env, "ioerr", "mmap");
        case SQLITE_IOERR_GETTEMPPATH:
            return make_two_atom_tuple(env, "ioerr", "gettemppath");
        case SQLITE_IOERR_CONVPATH:
            return make_two_atom_tuple(env, "ioerr", "convpath");
        case SQLITE_IOERR_VNODE:
            return make_two_atom_tuple(env, "ioerr", "vnode");
        case SQLITE_IOERR_AUTH:
            return make_two_atom_tuple(env, "ioerr", "auth");
        case SQLITE_IOERR_BEGIN_ATOMIC:
            return make_two_atom_tuple(env, "ioerr", "begin_atomic");
        case SQLITE_IOERR_COMMIT_ATOMIC:
            return make_two_atom_tuple(env, "ioerr", "commit_atomic");
        case SQLITE_IOERR_ROLLBACK_ATOMIC:
            return make_two_atom_tuple(env, "ioerr", "rollback_atomic");
        case SQLITE_IOERR_DATA:
            return make_two_atom_tuple(env, "ioerr", "data");
        case SQLITE_IOERR_CORRUPTFS:
            return make_two_atom_tuple(env, "ioerr", "corruptfs");
        case SQLITE_LOCKED_SHAREDCACHE:
            return make_two_atom_tuple(env, "locked", "sharedcache");
        case SQLITE_LOCKED_VTAB:
            return make_two_atom_tuple(env, "locked", "vtab");
        case SQLITE_BUSY_RECOVERY:
            return make_two_atom_tuple(env, "busy", "recovery");
        case SQLITE_BUSY_SNAPSHOT:
            return make_two_atom_tuple(env, "busy", "snapshot");
        case SQLITE_BUSY_TIMEOUT:
            return make_two_atom_tuple(env,"busy", "timeout");
        case SQLITE_CANTOPEN_NOTEMPDIR:
            return make_two_atom_tuple(env, "cantopen", "notempdir");
        case SQLITE_CANTOPEN_ISDIR:
            return make_two_atom_tuple(env,  "cantopen", "isdir");
        case SQLITE_CANTOPEN_FULLPATH:
            return make_two_atom_tuple(env, "cantopen", "fullpath");
        case SQLITE_CANTOPEN_CONVPATH:
            return make_two_atom_tuple(env, "cantopen", "convpath");
        case SQLITE_CANTOPEN_DIRTYWAL:
            return make_two_atom_tuple(env,  "cantopen", "dirtywal");
        case SQLITE_CANTOPEN_SYMLINK:
            return make_two_atom_tuple(env, "cantopen", "symlink");
        case SQLITE_CORRUPT_VTAB:
            return make_two_atom_tuple(env, "corrupt", "vtab");
        case SQLITE_CORRUPT_SEQUENCE:
            return make_two_atom_tuple(env, "corrupt", "sequence");
        case SQLITE_CORRUPT_INDEX:
            return make_two_atom_tuple(env, "corrupt", "index");
        case SQLITE_READONLY_RECOVERY:
            return make_two_atom_tuple(env, "readonly", "recovery");
        case SQLITE_READONLY_CANTLOCK:
            return make_two_atom_tuple(env,  "readonly", "cantlock");
        case SQLITE_READONLY_ROLLBACK:
            return make_two_atom_tuple(env, "readonly", "rollback");
        case SQLITE_READONLY_DBMOVED:
            return make_two_atom_tuple(env, "readonly", "dbmoved");
        case SQLITE_READONLY_CANTINIT:
            return make_two_atom_tuple(env, "readonly", "cantinit");
        case SQLITE_READONLY_DIRECTORY:
            return make_two_atom_tuple(env, "readonly", "directory");
        case SQLITE_ABORT_ROLLBACK:
            return make_two_atom_tuple(env, "abort", "rollback");
        case SQLITE_CONSTRAINT_CHECK:
            return make_two_atom_tuple(env, "constraint", "check");
        case SQLITE_CONSTRAINT_COMMITHOOK:
            return make_two_atom_tuple(env, "constraint", "commithook");
        case SQLITE_CONSTRAINT_FOREIGNKEY:
            return make_two_atom_tuple(env, "constraint", "foreignkey");
        case SQLITE_CONSTRAINT_FUNCTION:
            return make_two_atom_tuple(env, "constraint", "function");
        case SQLITE_CONSTRAINT_NOTNULL:
            return make_two_atom_tuple(env, "constraint", "notnull");
        case SQLITE_CONSTRAINT_PRIMARYKEY:
            return make_two_atom_tuple(env, "constraint", "primarykey");
        case SQLITE_CONSTRAINT_TRIGGER:
            return make_two_atom_tuple(env, "constraint", "trigger");
        case SQLITE_CONSTRAINT_UNIQUE:
            return make_two_atom_tuple(env, "constraint", "unique");
        case SQLITE_CONSTRAINT_VTAB:
            return make_two_atom_tuple(env, "constraint", "vtab");
        case SQLITE_CONSTRAINT_ROWID:
            return make_two_atom_tuple(env,  "constraint", "rowid");
        case SQLITE_CONSTRAINT_PINNED:
            return make_two_atom_tuple(env, "constraint", "pinned");
        case SQLITE_CONSTRAINT_DATATYPE:
            return make_two_atom_tuple(env, "constraint", "datatype");
        case SQLITE_NOTICE_RECOVER_WAL:
            return make_two_atom_tuple(env, "notice", "recover_wal");
        case SQLITE_NOTICE_RECOVER_ROLLBACK:
            return make_two_atom_tuple(env, "notice", "recover_rollback");
        case SQLITE_WARNING_AUTOINDEX:
            return make_two_atom_tuple(env, "warning", "autoindex");
        case SQLITE_AUTH_USER:
            return make_two_atom_tuple(env, "auth", "user");
        case SQLITE_OK_LOAD_PERMANENTLY:
            return make_two_atom_tuple(env, "ok", "load_permanently");
        case SQLITE_OK_SYMLINK:
            /* internal use only */
            return make_two_atom_tuple(env, "ok", "symlink");
    }
    return enif_make_tuple2(env, make_atom(env, "error"), enif_make_int(env, code));
}

static const char *
get_sqlite3_error_msg(int error_code, sqlite3 *db)
{
    static const char *msg;

    if(error_code == SQLITE_MISUSE)
        return "Sqlite3 was invoked incorrectly.";

    msg = sqlite3_errmsg(db);
    if(!msg)
        return "No sqlite3 error message found.";

    return msg;
}

static ERL_NIF_TERM
make_sqlite3_error_tuple(ErlNifEnv *env, int error_code) {
    return enif_make_tuple2(env, make_atom(env, "error"), make_extended_error_tuple(env, error_code));
}

/*
 *
 */
static void
destruct_esqlite3(ErlNifEnv *env, void *arg)
{
    esqlite3 *db = (esqlite3 *) arg;
    sqlite3_close_v2(db->db);
    db->db = NULL;
}

static void
destruct_esqlite3_stmt(ErlNifEnv *env, void *arg)
{
    esqlite3_stmt *stmt = (esqlite3_stmt *) arg;
    sqlite3_finalize(stmt->statement);
    stmt->statement = NULL;
}

static void
destruct_esqlite3_backup(ErlNifEnv *env, void *arg) 
{
    esqlite3_backup *backup = (esqlite3_backup *) arg;
    
    if(backup->backup) {
        sqlite3_backup_finish(backup->backup);
    }

    backup->backup = NULL;
}

/*
static ERL_NIF_TERM
do_set_update_hook(ErlNifEnv *env, esqlite3 *conn, const ERL_NIF_TERM arg)
{
    if(!enif_get_local_pid(env, arg, &conn->notification_pid)) {
        return make_error_tuple(env, "invalid_pid");
    }

    if(!conn->db) {
        return make_error_tuple(env, "closed");
    }

    sqlite3_update_hook(conn->db, update_callback, conn);

    return make_atom(env, "ok");
}
*/


/*
static int
bind_cell(ErlNifEnv *env, const ERL_NIF_TERM cell, sqlite3_stmt *stmt, unsigned int i)
{
    int the_int;
    ErlNifSInt64 the_long_int;
    double the_double;
    char the_atom[MAX_ATOM_LENGTH+1];
    ErlNifBinary the_blob;
    int arity;
    const ERL_NIF_TERM* tuple;

    if(enif_get_int(env, cell, &the_int))
        return sqlite3_bind_int(stmt, i, the_int);

    if(enif_get_int64(env, cell, &the_long_int))
        return sqlite3_bind_int64(stmt, i, the_long_int);

    if(enif_get_double(env, cell, &the_double))
        return sqlite3_bind_double(stmt, i, the_double);

    if(enif_get_atom(env, cell, the_atom, sizeof(the_atom), ERL_NIF_LATIN1)) {
        if(strncmp("undefined", the_atom, strlen("undefined")) == 0) {
            return sqlite3_bind_null(stmt, i);
        }  

        if(strncmp("null", the_atom, strlen("null")) == 0) {
            return sqlite3_bind_null(stmt, i);
        }

        return sqlite3_bind_text(stmt, i, the_atom, strlen(the_atom), SQLITE_TRANSIENT);
    }

    // Bind as text assume it is utf-8 encoded text
    if(enif_inspect_iolist_as_binary(env, cell, &the_blob)) {
        return sqlite3_bind_text(stmt, i, (char *) the_blob.data, the_blob.size, SQLITE_TRANSIENT);
    }

    // Check for blob tuple 
    if(enif_get_tuple(env, cell, &arity, &tuple)) {
        if(arity != 2)
            return -1;

        // length 2! 
        if(enif_get_atom(env, tuple[0], the_atom, sizeof(the_atom), ERL_NIF_LATIN1)) {
            // its a blob... 
            if(0 == strncmp("blob", the_atom, strlen("blob"))) {
                // with a iolist as argument 
                if(enif_inspect_iolist_as_binary(env, tuple[1], &the_blob)) {
                    // kaboom... get the blob 
                    return sqlite3_bind_blob(stmt, i, the_blob.data, the_blob.size, SQLITE_TRANSIENT);
                }
            }
        }
    }

    return -1;
}
*/

/*
static ERL_NIF_TERM
do_bind(ErlNifEnv *env, sqlite3 *db, sqlite3_stmt *stmt, const ERL_NIF_TERM arg)
{
    int parameter_count = sqlite3_bind_parameter_count(stmt);
    int i, is_list, r;
    ERL_NIF_TERM list, head, tail;
    unsigned int list_length;

    is_list = enif_get_list_length(env, arg, &list_length);
    if(!is_list)
        return make_error_tuple(env, "bad_arg_list");
    if(parameter_count != list_length)
        return make_error_tuple(env, "args_wrong_length");

    sqlite3_reset(stmt);

    list = arg;
    for(i=0; i < list_length; i++) {
        enif_get_list_cell(env, list, &head, &tail);
        r = bind_cell(env, head, stmt, i+1);
        if(r == -1)
            return make_error_tuple(env, "wrong_type");
        if(r != SQLITE_OK)
            return make_sqlite3_error_tuple(env, r, db);
        list = tail;
    }

    return make_atom(env, "ok");
}
*/

/*
static ERL_NIF_TERM
do_get_autocommit(ErlNifEnv *env, esqlite3 *conn)
{
    if(!conn->db) {
        return make_error_tuple(env, "closed");
    }

    if(sqlite3_get_autocommit(conn->db) != 0) {
        return make_atom(env, "true");
    } 

    return make_atom(env, "false");
}
*/

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const void *bytes, unsigned int size)
{
    ErlNifBinary blob;
    ERL_NIF_TERM term;

    if(!enif_alloc_binary(size, &blob)) {
        return make_atom(env, "error");
    }

    memcpy(blob.data, bytes, size);
    term = enif_make_binary(env, &blob);
    enif_release_binary(&blob);

    return term;
}

static ERL_NIF_TERM
make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i)
{
    int type = sqlite3_column_type(statement, i);

    switch(type) {
        case SQLITE_NULL:
            return make_atom(env, "undefined");
        case SQLITE_INTEGER:
            return enif_make_int64(env, sqlite3_column_int64(statement, i));
        case SQLITE_FLOAT:
            return enif_make_double(env, sqlite3_column_double(statement, i));
        case SQLITE_BLOB:
            return make_binary(env, sqlite3_column_blob(statement, i),
                        sqlite3_column_bytes(statement, i));
        case SQLITE_TEXT:
            return make_binary(env, sqlite3_column_text(statement, i),
                    sqlite3_column_bytes(statement, i));
    }

    return enif_raise_exception(env, make_atom(env, "internal_error"));
}

/*
static ERL_NIF_TERM
do_backup_init(ErlNifEnv *env, sqlite3 *db, const ERL_NIF_TERM arg)
{
    int tuple_arity;
    const ERL_NIF_TERM *elements;
    sqlite3_backup *backup;
    unsigned int size;
    char dst_name[MAX_SQLITE_NAME_LENGTH];
    char src_name[MAX_SQLITE_NAME_LENGTH];
    esqlite3 *src;
    esqlite3_backup *esqlite3_backup;
    ERL_NIF_TERM erl_backup_term;

    if(db == NULL) {
        return make_error_tuple(env, "dst_closed");
    }

    if(!enif_get_tuple(env, arg, &tuple_arity, &elements)) {
        return make_error_tuple(env, "no_tuple");
    }
    if(tuple_arity != 3) {
        return make_error_tuple(env, "invalid_tuple");
    }

    size = enif_get_string(env, elements[0], dst_name, MAX_PATHNAME, ERL_NIF_LATIN1);
    if(size <= 0)
        return make_error_tuple(env, "invalid_dst_name");

    if(!enif_get_resource(env, elements[1], esqlite3_type, (void **) &src)) {
        return make_error_tuple(env, "invalid_src_db");
    }
    if(!src->db) {
        return make_error_tuple(env, "src_closed");
    }
	
    size = enif_get_string(env, elements[2], src_name, MAX_PATHNAME, ERL_NIF_LATIN1);
    if(size <= 0)
        return make_error_tuple(env, "invalid_src_name");

    backup = sqlite3_backup_init(db, dst_name, src->db, src_name);
    if(backup == NULL) {
        return make_sqlite3_error_tuple(env, sqlite3_errcode(db), db);
    }

    esqlite3_backup = enif_alloc_resource(esqlite3_backup_type, sizeof(esqlite3_backup));
    if(!esqlite3_backup) {
        // Release backup resouces 
        (void) sqlite3_backup_finish(backup);
        return make_error_tuple(env, "no_memory");
    }

    esqlite3_backup->backup = backup;
    erl_backup_term = enif_make_resource(env, esqlite3_backup);
    enif_release_resource(esqlite3_backup);

    return make_ok_tuple(env, erl_backup_term);
}
*/

/*
static ERL_NIF_TERM
do_backup_step(ErlNifEnv *env, sqlite3 *db, const ERL_NIF_TERM arg)
{
    int tuple_arity;
    const ERL_NIF_TERM *elements;
    esqlite3_backup *esqlite3_backup;
    int n_page = 0;
    int rc;

    if(db == NULL) {
        return make_error_tuple(env, "closed");
    }

    if(!enif_get_tuple(env, arg, &tuple_arity, &elements)) {
        return make_error_tuple(env, "no_tuple");
    }
    if(tuple_arity != 2) {
        return make_error_tuple(env, "invalid_tuple");
    }

    if(!enif_get_resource(env, elements[0], esqlite3_backup_type, (void **) &esqlite3_backup)) {
        return make_error_tuple(env, "invalid");
    }
    if(!esqlite3_backup->backup) {
        return make_error_tuple(env, "backup");
    }

    if(!enif_get_int(env, elements[1], &n_page)) {
        return make_error_tuple(env, "n_page");
    }

    rc = sqlite3_backup_step(esqlite3_backup->backup, n_page);
    if(rc == SQLITE_DONE) {
        return make_atom(env, "done");
    }

    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc, db);
    }

    return make_atom(env, "ok");
}
*/

/*
static ERL_NIF_TERM
do_backup_remaining(ErlNifEnv *env, const ERL_NIF_TERM arg)
{
    esqlite3_backup *esqlite3_backup;
    int remaining;
    ERL_NIF_TERM remaining_term;

    if(!enif_get_resource(env, arg, esqlite3_backup_type, (void **) &esqlite3_backup)) {
        return make_error_tuple(env, "invalid");
    }

    remaining = sqlite3_backup_remaining(esqlite3_backup->backup);
    remaining_term = enif_make_int64(env, remaining);

    return make_ok_tuple(env, remaining_term);
}
*/

/*
static ERL_NIF_TERM
do_backup_pagecount(ErlNifEnv *env, const ERL_NIF_TERM arg)
{
    esqlite3_backup *esqlite3_backup;
    int pagecount;
    ERL_NIF_TERM pagecount_term;

    if(!enif_get_resource(env, arg, esqlite3_backup_type, (void **) &esqlite3_backup)) {
        return make_error_tuple(env, "invalid");
    }

    pagecount = sqlite3_backup_pagecount(esqlite3_backup->backup);
    pagecount_term = enif_make_int64(env, pagecount);

    return make_ok_tuple(env, pagecount_term);
}
*/

/*
static ERL_NIF_TERM
do_backup_finish(ErlNifEnv *env, const ERL_NIF_TERM arg)
{
    esqlite3_backup *esqlite3_backup;

    if(!enif_get_resource(env, arg, esqlite3_backup_type, (void **) &esqlite3_backup)) {
        return make_error_tuple(env, "invalid");
    }

    if(esqlite3_backup->backup) {
        (void) sqlite3_backup_finish(esqlite3_backup->backup);
        esqlite3_backup->backup = NULL;
    }

    return make_atom(env, "ok");
}
*/

/*
 * Open the database
 */
static ERL_NIF_TERM
esqlite_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    char filename[MAX_PATHNAME];

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!sqlite3_threadsafe()) {
        return enif_raise_exception(env, make_atom(env, "not_thread_safe"));
    }

    int size = enif_get_string(env, argv[0], filename, MAX_PATHNAME, ERL_NIF_LATIN1);
    if(size <= 0) {
        return make_error_tuple(env, "invalid_filename");
    }

    /* Initialize the resource */
    conn = enif_alloc_resource(esqlite3_type, sizeof(esqlite3));
    if(!conn) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));   
    }

     /* Open the database.
     */
    int rc = sqlite3_open(filename, &conn->db);
    if(rc != SQLITE_OK) {
        ERL_NIF_TERM error = make_sqlite3_error_tuple(env, rc);
        sqlite3_close_v2(conn->db);
        enif_release_resource(conn);
        return error;
    }

    /* Set a standard busy timeout of 2 seconds */
    sqlite3_busy_timeout(conn->db, 2000);

    /* Turn on extended error codes for better error reporting */
    sqlite3_extended_result_codes(conn->db, 1);
    
    ERL_NIF_TERM  db_conn = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return make_ok_tuple(env, db_conn);
}

/*
 * Close the database
 */
static ERL_NIF_TERM
esqlite_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    int rc;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    rc = sqlite3_close_v2(conn->db);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    conn->db = NULL;
    return make_atom(env, "ok");
}

void
update_callback(void *arg, int sqlite_operation_type, char const *sqlite_database, char const *sqlite_table, sqlite3_int64 sqlite_rowid)
{
    esqlite3 *conn = (esqlite3 *)arg;

    if(conn == NULL) {
        return;
    }

    /* Create a message environment */
    ErlNifEnv *msg_env = enif_alloc_env();

    ERL_NIF_TERM type;

    switch(sqlite_operation_type) {
        case SQLITE_INSERT:
            type = make_atom(msg_env, "insert");
            break;
        case SQLITE_DELETE:
            type = make_atom(msg_env, "delete");
            break;
        case SQLITE_UPDATE:
            type = make_atom(msg_env, "update");
            break;
        default:
            return;
    }
    ERL_NIF_TERM rowid = enif_make_int64(msg_env, sqlite_rowid);
    ERL_NIF_TERM database = make_binary(msg_env, sqlite_database, strlen(sqlite_database));
    ERL_NIF_TERM table = make_binary(msg_env, sqlite_table, strlen(sqlite_table));

    ERL_NIF_TERM msg = enif_make_tuple4(msg_env, type, database, table, rowid);

    if(!enif_send(NULL, &conn->update_hook_pid, msg_env, msg)) {
        sqlite3_update_hook(conn->db, NULL, NULL);
    }
}


static ERL_NIF_TERM
esqlite_set_update_hook(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    int rc;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    if(!conn->db) {
        return make_error_tuple(env, "closed");
    }

    if(enif_is_atom(env, argv[1])) {
        /* Assume this is undefined, reset the connection */
        sqlite3_update_hook(conn->db, NULL, NULL);
    } else {
        /* [todo] passing undefined resets the hook? */
        if(!enif_get_local_pid(env, argv[1], &conn->update_hook_pid)) {
            return enif_make_badarg(env);
        }

        sqlite3_update_hook(conn->db, update_callback, conn);
    }

    return make_atom(env, "ok");
}


/*
 * Exec a sql statement
 */
static ERL_NIF_TERM
esqlite_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    ErlNifBinary bin;
    int rc;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &bin)) {
        return enif_make_badarg(env);
    }

    rc = sqlite3_exec(conn->db, (char *) bin.data, NULL, NULL, NULL);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}


/*
 * Prepare the sql statement
 */
static ERL_NIF_TERM
esqlite_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    ErlNifBinary bin;
    esqlite3_stmt *stmt;
    ERL_NIF_TERM esqlite_stmt;
    const char *tail;
    int rc;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &bin)) {
        return enif_make_badarg(env);
    }

    unsigned int prep_flags;
    if(!enif_get_uint(env, argv[2], &prep_flags)) {
        return enif_make_badarg(env);
    }

    stmt = enif_alloc_resource(esqlite3_stmt_type, sizeof(esqlite3_stmt));
    if(!stmt) {
        return enif_raise_exception(env, make_atom(env, "no_memory"));   
    }

    rc = sqlite3_prepare_v3(conn->db, (char *) bin.data, bin.size, prep_flags, &(stmt->statement), &tail);
    if(rc != SQLITE_OK) {
        enif_release_resource(stmt);
        return make_sqlite3_error_tuple(env, rc);
    }

    stmt->column_count = sqlite3_column_count(stmt->statement);

    esqlite_stmt = enif_make_resource(env, stmt);
    enif_release_resource(stmt);

    return make_ok_tuple(env, esqlite_stmt);
}


/*
static ERL_NIF_TERM
set_update_hook(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &db))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");

    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    // command 
    cmd->type = cmd_update_hook_set;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, db, cmd);
}
*/


/*
 * Get the column names of the prepared statement.
 */
static ERL_NIF_TERM
esqlite_column_names(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    ERL_NIF_TERM column_names = enif_make_list(env, 0);

    int size = sqlite3_column_count(stmt->statement);
    if(size < 0) {
        return enif_raise_exception(env, make_atom(env, "invalid_column_count"));   
    }

    for(int i=size; i-- > 0; ) {
        const char *name = sqlite3_column_name(stmt->statement, i);
        if(name == NULL) {
            return enif_raise_exception(env, make_atom(env, "sqlite3_malloc_failure"));   
        }

        ERL_NIF_TERM ename = make_binary(env, name, strlen(name));
        column_names = enif_make_list_cell(env, ename, column_names);
    }

    return column_names;
}

/*
 * Get the column's declared datatypes of a statement.
 */
static ERL_NIF_TERM
esqlite_column_decltypes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    ERL_NIF_TERM column_types = enif_make_list(env, 0);

    int size = sqlite3_column_count(stmt->statement);
    if(size < 0) {
        return enif_raise_exception(env, make_atom(env, "invalid_column_count"));   
    }

    for(int i=size; i-- > 0; ) {
        ERL_NIF_TERM type_name;

        const char *type = sqlite3_column_decltype(stmt->statement, i);

        if(type == NULL) {
            type_name = make_atom(env, "undefined");
        } else {
            type_name = make_binary(env, type, strlen(type));
        }

        column_types = enif_make_list_cell(env, type_name, column_types);
    }

    return column_types;
}

static ERL_NIF_TERM
esqlite_bind_int(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    int index;
    int value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_get_int(env, argv[1], &index)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    int rc = sqlite3_bind_int(stmt->statement, index, value);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_bind_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    int index;
    ErlNifSInt64 value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_get_int(env, argv[1], &index)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int64(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    int rc = sqlite3_bind_int64(stmt->statement, index, value);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_bind_double(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    int index;
    double value;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_get_int(env, argv[1], &index)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_double(env, argv[2], &value)) {
        return enif_make_badarg(env);
    }

    int rc = sqlite3_bind_double(stmt->statement, index, value);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_bind_text(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    int index;
    ErlNifBinary text;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_get_int(env, argv[1], &index)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, argv[1], &text)) {
        return enif_make_badarg(env);
    }

    /* 
     * Don't do any checks on the input data, sqlite handes all kinds of input. It is
     * garbage-in, garbage-out.
     * 
     */

    int rc = sqlite3_bind_text64(stmt->statement, index, text.data, text.size, SQLITE_TRANSIENT, SQLITE_UTF8);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_bind_blob(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    int index;
    ErlNifBinary blob;

    if(argc != 3) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_get_int(env, argv[1], &index)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, argv[1], &blob)) {
        return enif_make_badarg(env);
    }

    int rc = sqlite3_bind_blob64(stmt->statement, index, blob.data, blob.size, SQLITE_TRANSIENT);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_bind_null(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    int index;
    double value;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_get_int(env, argv[1], &index)) {
        return enif_make_badarg(env);
    }

    int rc = sqlite3_bind_null(stmt->statement, index);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    int rc = sqlite3_step(stmt->statement);
    switch(rc) {
        case SQLITE_ROW:
            {
                ERL_NIF_TERM row = enif_make_list(env, 0);
                for(int i=stmt->column_count; i-- > 0; ) {
                    row = enif_make_list_cell(env, make_cell(env, stmt->statement, i), row);
                }
                return row;
            }
        case SQLITE_DONE:
            /* since  3.6.23.1 it is no longer required to do an explict reset.
             */
            return make_atom(env, "$done");
        case SQLITE_BUSY:
            return make_atom(env, "$busy");
    }

    return make_sqlite3_error_tuple(env, rc);
}

static ERL_NIF_TERM
esqlite_reset(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    int rc = sqlite3_reset(stmt->statement);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }
    return make_atom(env, "ok");
}


/*
 * Backup functions
 *
 */

/*
static ERL_NIF_TERM
esqlite_backup_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *destination;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 6)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &destination))
        return enif_make_badarg(env);
    // 1 destination name
    // 2 source connection with database
    // 3 source name
    if(!enif_is_ref(env, argv[4]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[5], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_init;
    cmd->ref = enif_make_copy(cmd->env, argv[4]);
    cmd->pid = pid;
    cmd->arg = enif_make_tuple3(cmd->env, argv[1], argv[2], argv[3]);

    // Use the connection of the destination database 
    return push_command(env, destination, cmd);
}
*/

/*
static ERL_NIF_TERM
esqlite_backup_finish(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    esqlite3_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_finish;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[1]);

    return push_command(env, conn, cmd);
}
*/

/*
static ERL_NIF_TERM
esqlite_backup_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    esqlite3_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 5)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);
    // 1 backup
    if(!enif_is_number(env, argv[2]))
        return make_error_tuple(env, "invalid_count");
    if(!enif_is_ref(env, argv[3]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[4], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_step;
    cmd->ref = enif_make_copy(cmd->env, argv[3]);
    cmd->pid = pid;
    cmd->arg = enif_make_tuple2(cmd->env, argv[1], argv[2]);

    return push_command(env, conn, cmd);
}
*/

/*
 * Get the remaining pagecount of the backup.

static ERL_NIF_TERM
esqlite_backup_remaining(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    esqlite3_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);
    // backup 1
    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_remaining;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[1]);

    return push_command(env, conn, cmd);
}
 */

/*
 * Get the total pagecount of the backup

static ERL_NIF_TERM
esqlite_backup_pagecount(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;
    esqlite3_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);
    // backup 1
    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_pagecount;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[1]);

    return push_command(env, conn, cmd);
}
 */

/*
 * Interrupt currently active query.
 */

static ERL_NIF_TERM
esqlite_interrupt(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;
    if(db->db == NULL) {
        return make_error_tuple(env, "closed");
    }

    sqlite3_interrupt(db->db);
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_get_autocommit(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;

    if(db->db == NULL) {
        return make_error_tuple(env, "closed");
    }

    if(sqlite3_get_autocommit(db->db)) {
        return make_atom(env, "true");
    } 

    return make_atom(env, "false");
}

static ERL_NIF_TERM
esqlite_last_insert_rowid(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;

    if(db->db == NULL) {
        return make_error_tuple(env, "closed");
    }

    sqlite3_int64 last_rowid = sqlite3_last_insert_rowid(db->db);
    return enif_make_int64(env, last_rowid);
}

static ERL_NIF_TERM
esqlite_changes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;

    if(db->db == NULL) {
        return make_error_tuple(env, "closed");
    }

    sqlite3_int64 changes = sqlite3_changes64(db->db);
    return enif_make_int64(env, changes);
}

/*
 * Load the nif. Initialize some stuff and such
 */
static int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt;

    rt = enif_open_resource_type(env, "esqlite3_nif", "esqlite3_type", destruct_esqlite3,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite3_type = rt;

    rt =  enif_open_resource_type(env, "esqlite3_nif", "esqlite3_stmt_type", destruct_esqlite3_stmt,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite3_stmt_type = rt;

    rt =  enif_open_resource_type(env, "esqlite3_nif", "esqlite3_backup_type", destruct_esqlite3_backup,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite3_backup_type = rt;

    atom_esqlite3 = make_atom(env, "esqlite3");

    if(SQLITE_OK != sqlite3_initialize()) {
        return -1;
    }

    return 0;
}

static int on_reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static int on_upgrade(ErlNifEnv* env, void** priv, void** old_priv_data, ERL_NIF_TERM load_info)
{
    return 0;
}

static ErlNifFunc nif_funcs[] = {
    {"open", 1, esqlite_open, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close", 1, esqlite_close, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"set_update_hook", 2, esqlite_set_update_hook},

    {"exec", 2, esqlite_exec},
    {"prepare", 3, esqlite_prepare},

    {"column_names", 1, esqlite_column_names},
    {"column_decltypes", 1, esqlite_column_decltypes},

    {"bind_int", 3, esqlite_bind_int},
    {"bind_int64", 3, esqlite_bind_int64},
    {"bind_double", 3, esqlite_bind_double},
    {"bind_text", 3, esqlite_bind_text},
    {"bind_blob", 3, esqlite_bind_blob},
    {"bind_null", 2, esqlite_bind_null},

    /*
    {"bind_text", 3, esqlite_bind_blob},
    */

    {"step", 1, esqlite_step},
    {"reset", 1, esqlite_reset},

    /*
     * Other interesting additions... trace.
     * wal_hook triggered after every commit.
     * also interesting commit and rollback_hooks
     */

    {"interrupt", 1, esqlite_interrupt, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"last_insert_rowid", 1, esqlite_last_insert_rowid},
    {"get_autocommit", 1, esqlite_get_autocommit},
    {"changes", 1, esqlite_changes},

    /*
    {"backup_init", 6, esqlite_backup_init},
    {"backup_step", 5, esqlite_backup_step},
    {"backup_remaining", 4, esqlite_backup_remaining},
    {"backup_pagecount", 4, esqlite_backup_pagecount},
    {"backup_finish", 4, esqlite_backup_finish},
    */

};

ERL_NIF_INIT(esqlite3_nif, nif_funcs, on_load, on_reload, on_upgrade, NULL);
