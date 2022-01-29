/*
 * Copyright 2011 - 2017 Maas-Maarten Zeeman
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
#include "queue.h"

#define MAX_ATOM_LENGTH 255         /* from atom.h, not exposed in erlang include */
#define MAX_SQLITE_NAME_LENGTH  255 /* Maximum name length. Using longer will return misuse */
#define MAX_PATHNAME 512            /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite_connection_type = NULL;
static ErlNifResourceType *esqlite_statement_type = NULL;
static ErlNifResourceType *esqlite_backup_type = NULL;

/* database connection context */
typedef struct {
    ErlNifTid tid;
    ErlNifThreadOpts* opts;
    ErlNifPid notification_pid;

    sqlite3 *db;
    queue *commands;

} esqlite_connection;

/* prepared statement */
typedef struct {
    sqlite3_stmt *statement;
} esqlite_statement;

/* data associated with ongoing backup */
typedef struct {
    sqlite3_backup *backup;
} esqlite_backup;

typedef enum {
    cmd_unknown,
    cmd_open,
    cmd_update_hook_set,
    cmd_notification,
    cmd_exec,
    cmd_changes,
    cmd_prepare,
    cmd_bind,
    cmd_multi_step,
    cmd_reset,
    cmd_column_names,
    cmd_column_types,
    cmd_backup_init,
    cmd_backup_step,
    cmd_backup_remaining,
    cmd_backup_pagecount,
    cmd_backup_finish,
    cmd_close,
    cmd_stop,
    cmd_insert,
    cmd_last_insert_rowid,
    cmd_get_autocommit,
} command_type;

typedef struct {
    command_type type;

    ErlNifEnv *env;
    ERL_NIF_TERM ref;
    ErlNifPid pid;
    ERL_NIF_TERM arg;
    ERL_NIF_TERM stmt;
} esqlite_command;

static ERL_NIF_TERM atom_esqlite3;

static ERL_NIF_TERM push_command(ErlNifEnv *env, esqlite_connection *conn, esqlite_command *cmd);

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
        case SQLITE_BUSY: return "busy";
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
        case SQLITE_DONE: return  "done";
    }
    
    return  "unknown";
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
make_sqlite3_error_tuple(ErlNifEnv *env, int error_code, sqlite3 *db)
{
    const char *error_code_msg = get_sqlite3_return_code_msg(error_code);
    const char *msg = get_sqlite3_error_msg(error_code, db);

    return enif_make_tuple2(env, make_atom(env, "error"),
        enif_make_tuple2(env, make_atom(env, error_code_msg),
            enif_make_string(env, msg, ERL_NIF_LATIN1)));
}

static void
command_destroy(void *obj)
{
    esqlite_command *cmd = (esqlite_command *) obj;

    if(cmd->env != NULL)
        enif_free_env(cmd->env);

    enif_free(cmd);
}

static esqlite_command *
command_create()
{
    esqlite_command *cmd = (esqlite_command *) enif_alloc(sizeof(esqlite_command));
    if(cmd == NULL)
        return NULL;

    cmd->env = enif_alloc_env();
    if(cmd->env == NULL) {
        command_destroy(cmd);
        return NULL;
    }

    cmd->type = cmd_unknown;
    cmd->ref = 0;
    cmd->arg = 0;
    cmd->stmt = 0;

    return cmd;
}

/*
 *
 */
static void
destruct_esqlite_connection(ErlNifEnv *env, void *arg)
{
    esqlite_connection *db = (esqlite_connection *) arg;
    esqlite_command *cmd = command_create();

    /* Send the stop command
     */
    cmd->type = cmd_stop;
    queue_push(db->commands, cmd);

    /* Wait for the thread to finish
     */
    enif_thread_join(db->tid, NULL);

    enif_thread_opts_destroy(db->opts);

    /* The thread has finished... now remove the command queue, and close
     * the database (if it was still open).
     */
    while(queue_has_item(db->commands)) {
        command_destroy(queue_pop(db->commands));
    }
    queue_destroy(db->commands);

    sqlite3_close_v2(db->db);
    db->db = NULL;
}

static void
destruct_esqlite_statement(ErlNifEnv *env, void *arg)
{
    esqlite_statement *stmt = (esqlite_statement *) arg;
    sqlite3_finalize(stmt->statement);
    stmt->statement = NULL;
}

static void
destruct_esqlite_backup(ErlNifEnv *env, void *arg) 
{
    esqlite_backup *backup = (esqlite_backup *) arg;
    
    if(backup->backup) {
        sqlite3_backup_finish(backup->backup);
    }

    backup->backup = NULL;
}

static ERL_NIF_TERM
do_open(ErlNifEnv *env, esqlite_connection *db, const ERL_NIF_TERM arg)
{
    char filename[MAX_PATHNAME];
    unsigned int size;
    int rc;
    ERL_NIF_TERM error;

    size = enif_get_string(env, arg, filename, MAX_PATHNAME, ERL_NIF_LATIN1);
    if(size <= 0)
        return make_error_tuple(env, "invalid_filename");

    /* Open the database.
     */
    rc = sqlite3_open(filename, &db->db);
    if(rc != SQLITE_OK) {
        error = make_sqlite3_error_tuple(env, rc, db->db);
        sqlite3_close_v2(db->db);
        db->db = NULL;

        return error;
    }

    sqlite3_busy_timeout(db->db, 2000);

    return make_atom(env, "ok");
}

void
update_callback(void *arg, int sqlite_operation_type, char const *sqlite_database, char const *sqlite_table, sqlite3_int64 sqlite_rowid)
{
    esqlite_connection *db = (esqlite_connection *)arg;
    esqlite_command *cmd = NULL;
    ERL_NIF_TERM type, table, rowid;

    if(db == NULL)
        return;

    cmd = command_create();
    if(!cmd)
        return;

    rowid = enif_make_int64(cmd->env, sqlite_rowid);
    table = enif_make_string(cmd->env, sqlite_table, ERL_NIF_LATIN1);

    switch(sqlite_operation_type) {
        case SQLITE_INSERT:
            type = make_atom(cmd->env, "insert");
            break;
        case SQLITE_DELETE:
            type = make_atom(cmd->env, "delete");
            break;
        case SQLITE_UPDATE:
            type = make_atom(cmd->env, "update");
            break;
        default:
            return;
    }

    cmd->type = cmd_notification;
    cmd->arg = enif_make_tuple3(cmd->env, type, table, rowid);

    push_command(cmd->env, db, cmd);
}

static ERL_NIF_TERM
do_set_update_hook(ErlNifEnv *env, esqlite_connection *conn, const ERL_NIF_TERM arg)
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

/*
 */
static ERL_NIF_TERM
do_exec(ErlNifEnv *env, esqlite_connection *conn, const ERL_NIF_TERM arg)
{
    ErlNifBinary bin;
    int rc;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, arg, eos), &bin)) {
        return make_error_tuple(env, "no_iodata");
    }

    rc = sqlite3_exec(conn->db, (char *) bin.data, NULL, NULL, NULL);
    if(rc != SQLITE_OK)
        return make_sqlite3_error_tuple(env, rc, conn->db);

    return make_atom(env, "ok");
}

/*
 * Nr of changes
 */
static ERL_NIF_TERM
do_changes(ErlNifEnv *env, esqlite_connection *conn, const ERL_NIF_TERM arg)
{
    if(!conn->db) {
        return make_error_tuple(env, "closed");
    }

    sqlite3_int64 changes = sqlite3_changes64(conn->db);
    ERL_NIF_TERM changes_term = enif_make_int64(env, changes);

    return make_ok_tuple(env, changes_term);
}

/*
* insert action
*/
static ERL_NIF_TERM
do_insert(ErlNifEnv *env, esqlite_connection *conn, const ERL_NIF_TERM arg)
{
    ErlNifBinary bin;
    int rc;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, arg, eos), &bin)) {
        return make_error_tuple(env, "no_iodata");
    }

    rc = sqlite3_exec(conn->db, (char *) bin.data, NULL, NULL, NULL);
    if(rc != SQLITE_OK)
        return make_sqlite3_error_tuple(env, rc, conn->db);
    sqlite3_int64 last_rowid = sqlite3_last_insert_rowid(conn->db);
    ERL_NIF_TERM last_rowid_term = enif_make_int64(env, last_rowid);
    return make_ok_tuple(env, last_rowid_term);
}

/*
 * Return the last inserted rowid
 */
static ERL_NIF_TERM
do_last_insert_rowid(ErlNifEnv *env, esqlite_connection *conn)
{
    if(!conn->db) {
        return make_error_tuple(env, "closed");
    }

    sqlite3_int64 last_rowid = sqlite3_last_insert_rowid(conn->db);
    ERL_NIF_TERM last_rowid_term = enif_make_int64(env, last_rowid);

    return make_ok_tuple(env, last_rowid_term);
}

/*
 * Compile a sql statement
 */
static ERL_NIF_TERM
do_prepare(ErlNifEnv *env, esqlite_connection *conn, const ERL_NIF_TERM arg)
{
    ErlNifBinary bin;
    esqlite_statement *stmt;
    ERL_NIF_TERM esqlite_stmt;
    const char *tail;
    int rc;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, arg, eos), &bin)) {
        return make_error_tuple(env, "no_iodata");
    }

    stmt = enif_alloc_resource(esqlite_statement_type, sizeof(esqlite_statement));
    if(!stmt)
        return make_error_tuple(env, "no_memory");

    rc = sqlite3_prepare_v2(conn->db, (char *) bin.data, bin.size, &(stmt->statement), &tail);
    if(rc != SQLITE_OK) {
        enif_release_resource(stmt);
        return make_sqlite3_error_tuple(env, rc, conn->db);
    }

    esqlite_stmt = enif_make_resource(env, stmt);
    enif_release_resource(stmt);

    return make_ok_tuple(env, esqlite_stmt);
}

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

    /* Bind as text assume it is utf-8 encoded text */
    if(enif_inspect_iolist_as_binary(env, cell, &the_blob)) {
        return sqlite3_bind_text(stmt, i, (char *) the_blob.data, the_blob.size, SQLITE_TRANSIENT);
    }

    /* Check for blob tuple */
    if(enif_get_tuple(env, cell, &arity, &tuple)) {
        if(arity != 2)
            return -1;

        /* length 2! */
        if(enif_get_atom(env, tuple[0], the_atom, sizeof(the_atom), ERL_NIF_LATIN1)) {
            /* its a blob... */
            if(0 == strncmp("blob", the_atom, strlen("blob"))) {
                /* with a iolist as argument */
                if(enif_inspect_iolist_as_binary(env, tuple[1], &the_blob)) {
                    /* kaboom... get the blob */
                    return sqlite3_bind_blob(stmt, i, the_blob.data, the_blob.size, SQLITE_TRANSIENT);
                }
            }
        }
    }

    return -1;
}

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

static ERL_NIF_TERM
do_get_autocommit(ErlNifEnv *env, esqlite_connection *conn)
{
    if(!conn->db) {
        return make_error_tuple(env, "closed");
    }

    if(sqlite3_get_autocommit(conn->db) != 0) {
        return make_atom(env, "true");
    } 

    return make_atom(env, "false");
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const void *bytes, unsigned int size)
{
    ErlNifBinary blob;
    ERL_NIF_TERM term;

    if(!enif_alloc_binary(size, &blob)) {
        /* TODO: fix this */
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
        case SQLITE_INTEGER:
            return enif_make_int64(env, sqlite3_column_int64(statement, i));
        case SQLITE_FLOAT:
            return enif_make_double(env, sqlite3_column_double(statement, i));
        case SQLITE_BLOB:
            return enif_make_tuple2(env, make_atom(env, "blob"),
                    make_binary(env, sqlite3_column_blob(statement, i),
                        sqlite3_column_bytes(statement, i)));
        case SQLITE_NULL:
            return make_atom(env, "undefined");
        case SQLITE_TEXT:
            return make_binary(env, sqlite3_column_text(statement, i),
                    sqlite3_column_bytes(statement, i));
        default:
            return make_atom(env, "should_not_happen");
    }
}

static ERL_NIF_TERM
make_row(ErlNifEnv *env, sqlite3_stmt *statement, ERL_NIF_TERM *array, int size)
{
    if(!array)
        return make_error_tuple(env, "no_memory");

    for(int i = 0; i < size; i++)
        array[i] = make_cell(env, statement, i);

    return enif_make_tuple_from_array(env, array, size);
}

static ERL_NIF_TERM
do_multi_step(ErlNifEnv *env, sqlite3 *db, sqlite3_stmt *stmt, const ERL_NIF_TERM arg)
{
    ERL_NIF_TERM status;
    ERL_NIF_TERM rows = enif_make_list_from_array(env, NULL, 0);
    ERL_NIF_TERM *rowBuffer = NULL;
    int rowBufferSize = 0;

    int chunk_size = 0;
    enif_get_int(env, arg, &chunk_size);

    int rc = sqlite3_step(stmt);
    while (rc == SQLITE_ROW && chunk_size-- > 0)
    {
        if (!rowBufferSize)
            rowBufferSize = sqlite3_column_count(stmt);
        if (rowBuffer == NULL)
            rowBuffer = (ERL_NIF_TERM *) enif_alloc(sizeof(ERL_NIF_TERM)*rowBufferSize);

        rows = enif_make_list_cell(env, make_row(env, stmt, rowBuffer, rowBufferSize), rows);

        if (chunk_size > 0)
            rc = sqlite3_step(stmt);
    }

    switch(rc) {
        case SQLITE_ROW:
            status = make_atom(env, "rows");
            break;
        case SQLITE_BUSY:
            status = make_atom(env, "$busy");
            break;
        case SQLITE_DONE:
            /*
             * Automatically reset the statement after a done so
             * column_names will work after the statement is done.
             *
             * Not resetting the statement can lead to vm crashes.
             */
            sqlite3_reset(stmt);
            status = make_atom(env, "$done");
            break;
        default:
            /* We use prepare_v2, so any error code can be returned. */
            return make_sqlite3_error_tuple(env, rc, db);
    }

    enif_free(rowBuffer);
    return enif_make_tuple2(env, status, rows);
}

static ERL_NIF_TERM
do_reset(ErlNifEnv *env, sqlite3 *db, sqlite3_stmt *stmt)
{
    int rc = sqlite3_reset(stmt);
    if(rc != SQLITE_OK)
        return make_sqlite3_error_tuple(env, rc, db);

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
do_column_names(ErlNifEnv *env, sqlite3_stmt *stmt)
{
    int i, size;
    const char *name;
    ERL_NIF_TERM *array;
    ERL_NIF_TERM column_names;

    size = sqlite3_column_count(stmt);
    if(size == 0)
        return enif_make_tuple(env, 0);
    else if(size < 0)
        return make_error_tuple(env, "invalid_column_count");

    array = (ERL_NIF_TERM *) enif_alloc(sizeof(ERL_NIF_TERM) * size);
    if(!array)
        return make_error_tuple(env, "no_memory");

    for(i = 0; i < size; i++) {
        name = sqlite3_column_name(stmt, i);
        if(name == NULL) {
            enif_free(array);
            return make_error_tuple(env, "sqlite3_malloc_failure");
        }

        array[i] = make_atom(env, name);
    }

    column_names = enif_make_tuple_from_array(env, array, size);
    enif_free(array);
    return column_names;
}

static ERL_NIF_TERM
do_column_types(ErlNifEnv *env, sqlite3_stmt *stmt)
{
    int i, size;
    const char *type;
    ERL_NIF_TERM *array;
    ERL_NIF_TERM column_types;

    size = sqlite3_column_count(stmt);
    if(size == 0)
        return enif_make_tuple(env, 0);
    else if(size < 0)
        return make_error_tuple(env, "invalid_column_count");

    array = (ERL_NIF_TERM *) enif_alloc(sizeof(ERL_NIF_TERM) * size);
    if(!array)
        return make_error_tuple(env, "no_memory");

    for(i = 0; i < size; i++) {
        type = sqlite3_column_decltype(stmt, i);
        if(type == NULL) {
	    type = "nil";
        }

        array[i] = make_atom(env, type);
    }

    column_types = enif_make_tuple_from_array(env, array, size);
    enif_free(array);
    return column_types;
}

static ERL_NIF_TERM
do_backup_init(ErlNifEnv *env, sqlite3 *db, const ERL_NIF_TERM arg)
{
    int tuple_arity;
    const ERL_NIF_TERM *elements;
    sqlite3_backup *backup;
    unsigned int size;
    char dst_name[MAX_SQLITE_NAME_LENGTH];
    char src_name[MAX_SQLITE_NAME_LENGTH];
    esqlite_connection *src;
    esqlite_backup *esqlite_backup;
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

    if(!enif_get_resource(env, elements[1], esqlite_connection_type, (void **) &src)) {
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

    esqlite_backup = enif_alloc_resource(esqlite_backup_type, sizeof(esqlite_backup));
    if(!esqlite_backup) {
        // Release backup resouces 
        (void) sqlite3_backup_finish(backup);
        return make_error_tuple(env, "no_memory");
    }

    esqlite_backup->backup = backup;
    erl_backup_term = enif_make_resource(env, esqlite_backup);
    enif_release_resource(esqlite_backup);

    return make_ok_tuple(env, erl_backup_term);
}

static ERL_NIF_TERM
do_backup_step(ErlNifEnv *env, sqlite3 *db, const ERL_NIF_TERM arg)
{
    int tuple_arity;
    const ERL_NIF_TERM *elements;
    esqlite_backup *esqlite_backup;
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

    if(!enif_get_resource(env, elements[0], esqlite_backup_type, (void **) &esqlite_backup)) {
        return make_error_tuple(env, "invalid");
    }
    if(!esqlite_backup->backup) {
        return make_error_tuple(env, "backup");
    }

    if(!enif_get_int(env, elements[1], &n_page)) {
        return make_error_tuple(env, "n_page");
    }

    rc = sqlite3_backup_step(esqlite_backup->backup, n_page);
    if(rc == SQLITE_DONE) {
        return make_atom(env, "done");
    }

    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc, db);
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
do_backup_remaining(ErlNifEnv *env, const ERL_NIF_TERM arg)
{
    esqlite_backup *esqlite_backup;
    int remaining;
    ERL_NIF_TERM remaining_term;

    if(!enif_get_resource(env, arg, esqlite_backup_type, (void **) &esqlite_backup)) {
        return make_error_tuple(env, "invalid");
    }

    remaining = sqlite3_backup_remaining(esqlite_backup->backup);
    remaining_term = enif_make_int64(env, remaining);

    return make_ok_tuple(env, remaining_term);
}

static ERL_NIF_TERM
do_backup_pagecount(ErlNifEnv *env, const ERL_NIF_TERM arg)
{
    esqlite_backup *esqlite_backup;
    int pagecount;
    ERL_NIF_TERM pagecount_term;

    if(!enif_get_resource(env, arg, esqlite_backup_type, (void **) &esqlite_backup)) {
        return make_error_tuple(env, "invalid");
    }

    pagecount = sqlite3_backup_pagecount(esqlite_backup->backup);
    pagecount_term = enif_make_int64(env, pagecount);

    return make_ok_tuple(env, pagecount_term);
}

static ERL_NIF_TERM
do_backup_finish(ErlNifEnv *env, const ERL_NIF_TERM arg)
{
    esqlite_backup *esqlite_backup;

    if(!enif_get_resource(env, arg, esqlite_backup_type, (void **) &esqlite_backup)) {
        return make_error_tuple(env, "invalid");
    }

    if(esqlite_backup->backup) {
        (void) sqlite3_backup_finish(esqlite_backup->backup);
        esqlite_backup->backup = NULL;
    }

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
do_close(ErlNifEnv *env, esqlite_connection *conn, const ERL_NIF_TERM arg)
{
    int rc;

    rc = sqlite3_close_v2(conn->db);
    if(rc != SQLITE_OK)
        return make_sqlite3_error_tuple(env, rc, conn->db);

    conn->db = NULL;
    return make_atom(env, "ok");
}

static ERL_NIF_TERM
evaluate_command(esqlite_command *cmd, esqlite_connection *conn)
{
    esqlite_statement *stmt = NULL;

    if(cmd->stmt) {
        if(!enif_get_resource(cmd->env, cmd->stmt, esqlite_statement_type, (void **) &stmt)) {
	    return make_error_tuple(cmd->env, "invalid_statement");
        }
    }

    switch(cmd->type) {
        case cmd_open:
            return do_open(cmd->env, conn, cmd->arg);
        case cmd_update_hook_set:
            return do_set_update_hook(cmd->env, conn, cmd->arg);
        case cmd_exec:
            return do_exec(cmd->env, conn, cmd->arg);
        case cmd_changes:
            return do_changes(cmd->env, conn, cmd->arg);
        case cmd_prepare:
            return do_prepare(cmd->env, conn, cmd->arg);
        case cmd_multi_step:
            return do_multi_step(cmd->env, conn->db, stmt->statement, cmd->arg);
        case cmd_reset:
            return do_reset(cmd->env, conn->db, stmt->statement);
        case cmd_bind:
            return do_bind(cmd->env, conn->db, stmt->statement, cmd->arg);
        case cmd_column_names:
            return do_column_names(cmd->env, stmt->statement);
        case cmd_column_types:
            return do_column_types(cmd->env, stmt->statement);
        case cmd_backup_init:
            return do_backup_init(cmd->env, conn->db, cmd->arg);
        case cmd_backup_step:
            return do_backup_step(cmd->env, conn->db, cmd->arg);
        case cmd_backup_remaining:
            return do_backup_remaining(cmd->env, cmd->arg);
        case cmd_backup_pagecount:
            return do_backup_pagecount(cmd->env, cmd->arg);
        case cmd_backup_finish:
            return do_backup_finish(cmd->env, cmd->arg);
        case cmd_close:
            return do_close(cmd->env, conn, cmd->arg);
        case cmd_last_insert_rowid:
            return do_last_insert_rowid(cmd->env, conn);
        case cmd_insert:
            return do_insert(cmd->env, conn, cmd->arg);
        case cmd_get_autocommit:
            return do_get_autocommit(cmd->env, conn);
        case cmd_unknown:      // not handled
        case cmd_stop:         // not handled here
        case cmd_notification: // not handled here.
            break;
    }

    return make_error_tuple(cmd->env, "invalid_command");
}

static ERL_NIF_TERM
push_command(ErlNifEnv *env, esqlite_connection *conn, esqlite_command *cmd) {
    if(!queue_push(conn->commands, cmd))
        return make_error_tuple(env, "command_push_failed");

    return make_atom(env, "ok");
}

static ERL_NIF_TERM
make_answer(esqlite_command *cmd, ERL_NIF_TERM answer)
{
    return enif_make_tuple3(cmd->env, atom_esqlite3, cmd->ref, answer);
}

static void *
esqlite_connection_run(void *arg)
{
    esqlite_connection *db = (esqlite_connection *) arg;
    esqlite_command *cmd;
    int continue_running = 1;

    while(continue_running) {
        cmd = queue_pop(db->commands);

        if(cmd->type == cmd_stop) {
            continue_running = 0;
        } else if(cmd->type == cmd_notification) {
            enif_send(NULL, &db->notification_pid, cmd->env, cmd->arg);
        } else {
            enif_send(NULL, &cmd->pid, cmd->env, make_answer(cmd, evaluate_command(cmd, db)));
        }

        command_destroy(cmd);
    }

    return NULL;
}

/*
 * Start the processing thread
 */
static ERL_NIF_TERM
esqlite_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    ERL_NIF_TERM db_conn;

    /* Initialize the resource */
    conn = enif_alloc_resource(esqlite_connection_type, sizeof(esqlite_connection));
    if(!conn)
        return make_error_tuple(env, "no_memory");

    conn->db = NULL;

    /* Create command queue */
    conn->commands = queue_create();
    if(!conn->commands) {
        enif_release_resource(conn);
        return make_error_tuple(env, "command_queue_create_failed");
    }

    /* Start command processing thread */
    conn->opts = enif_thread_opts_create("esqlite_thread_opts");
    if(conn->opts == NULL) {
        return make_error_tuple(env, "thread_opts_failed");
    }

    /* Configure a fixed sized stack, windows uses a default of 1Mb, which 
     * can be too small for complex queries. Linux and MacOS uses a stack of about
     * 8Mb, which is a bit too large, since the largest sqlite query is about 1Mb
     * in size. The stack size depends on that. A value of 3Mb is about right.
     */
    conn->opts->suggested_stack_size = 3072;

    if(enif_thread_create("esqlite_connection", &conn->tid, esqlite_connection_run, conn, conn->opts) != 0) {
        enif_thread_opts_destroy(conn->opts);
        enif_release_resource(conn);
        return make_error_tuple(env, "thread_create_failed");
    }

    db_conn = enif_make_resource(env, conn);
    enif_release_resource(conn);

    return make_ok_tuple(env, db_conn);
}

/*
 * Open the database
 */
static ERL_NIF_TERM
esqlite_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    if(!sqlite3_threadsafe())
        return make_error_tuple(env, "sqlite3 not thread safe.");

    /* Note, no check is made for the type of the argument */
    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_open;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, db, cmd);
}

static ERL_NIF_TERM
set_update_hook(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    /* command */
    cmd->type = cmd_update_hook_set;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, db, cmd);
}

/*
 * Execute the sql statement
 */
static ERL_NIF_TERM
esqlite_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    /* command */
    cmd->type = cmd_exec;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, db, cmd);
}

/*
 * Count the nr of changes of last statement
 */
static ERL_NIF_TERM
esqlite_changes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 3)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    /* command */
    cmd->type = cmd_changes;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(env, db, cmd);
}

static ERL_NIF_TERM
esqlite_insert(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    /* command */
    cmd->type = cmd_insert;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, db, cmd);
}

static ERL_NIF_TERM
esqlite_last_insert_rowid(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 3)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    /* command */
    cmd->type = cmd_last_insert_rowid;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(env, db, cmd);
}

static ERL_NIF_TERM
esqlite_get_autocommit(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 3)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    /* command */
    cmd->type = cmd_get_autocommit;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(env, db, cmd);
}

/*
 * Prepare the sql statement
 */
static ERL_NIF_TERM
esqlite_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_prepare;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(env, conn, cmd);
}

/*
 * Bind a variable to a prepared statement
 */
static ERL_NIF_TERM
esqlite_bind(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 5)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[1], esqlite_statement_type, (void **) &stmt))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_bind;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->stmt = enif_make_copy(cmd->env, argv[1]);
    cmd->arg = enif_make_copy(cmd->env, argv[4]);

    return push_command(env, conn, cmd);
}

/*
 * Multi step to a prepared statement
 */
static ERL_NIF_TERM
esqlite_multi_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
    int chunk_size = 0;

    if(argc != 5)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[1], esqlite_statement_type, (void **) &stmt))
        return enif_make_badarg(env);

    if(!enif_get_int(env, argv[2], &chunk_size))
        return make_error_tuple(env, "invalid_chunk_size");

    if(!enif_is_ref(env, argv[3]))
        return make_error_tuple(env, "invalid_ref");

    if(!enif_get_local_pid(env, argv[4], &pid))
        return make_error_tuple(env, "invalid_pid");

    if(!stmt->statement)
        return make_error_tuple(env, "no_prepared_statement");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_multi_step;
    cmd->ref = enif_make_copy(cmd->env, argv[3]);
    cmd->pid = pid;
    cmd->stmt = enif_make_copy(cmd->env, argv[1]);
    cmd->arg = enif_make_copy(cmd->env, argv[2]);

    return push_command(env, conn, cmd);
}

/*
 * Reset a prepared statement to its initial state
 */
static ERL_NIF_TERM
esqlite_reset(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[1], esqlite_statement_type, (void **) &stmt))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");
    if(!stmt->statement)
        return make_error_tuple(env, "no_prepared_statement");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_reset;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->stmt = enif_make_copy(cmd->env, argv[1]);

    return push_command(env, conn, cmd);
}

/*
 * Get the column names of the prepared statement.
 */
static ERL_NIF_TERM
esqlite_column_names(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[1], esqlite_statement_type, (void **) &stmt))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");
    if(!stmt->statement)
        return make_error_tuple(env, "no_prepared_statement");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_column_names;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->stmt = enif_make_copy(cmd->env, argv[1]);

    return push_command(env, conn, cmd);
}

/*
 * Get the column types of the prepared statement.
 */
static ERL_NIF_TERM
esqlite_column_types(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[1], esqlite_statement_type, (void **) &stmt))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[2]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid))
        return make_error_tuple(env, "invalid_pid");

    if(!stmt->statement)
        return make_error_tuple(env, "no_prepared_statement");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_column_types;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->stmt = enif_make_copy(cmd->env, argv[1]);

    return push_command(env, conn, cmd);
}

/*
 * Backup functions
 *
 */


static ERL_NIF_TERM
esqlite_backup_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *destination;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 6)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &destination))
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

    /* Use the connection of the destination database */
    return push_command(env, destination, cmd);
}

static ERL_NIF_TERM
esqlite_backup_finish(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
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

static ERL_NIF_TERM
esqlite_backup_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 5)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
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

/*
 * Get the remaining pagecount of the backup.
 */

static ERL_NIF_TERM
esqlite_backup_remaining(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
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

/*
 * Get the total pagecount of the backup
 */

static ERL_NIF_TERM
esqlite_backup_pagecount(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_backup *backup;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
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

/*
 * Interrupt currently active query.
 */

static ERL_NIF_TERM
esqlite_interrupt(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite_connection *db = (esqlite_connection *) conn;
    sqlite3_interrupt(db->db);

    return enif_make_atom(env, "ok");
}

/*
 * Close the database
 */
static ERL_NIF_TERM
esqlite_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1]))
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid))
        return make_error_tuple(env, "invalid_pid");

    cmd = command_create();
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_close;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(env, conn, cmd);
}

/*
 * Load the nif. Initialize some stuff and such
 */
static int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt;

    rt = enif_open_resource_type(env, "esqlite3_nif", "esqlite_connection_type", destruct_esqlite_connection,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite_connection_type = rt;

    rt =  enif_open_resource_type(env, "esqlite3_nif", "esqlite_statement_type", destruct_esqlite_statement,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite_statement_type = rt;

    rt =  enif_open_resource_type(env, "esqlite3_nif", "esqlite_backup_type", destruct_esqlite_backup,
            ERL_NIF_RT_CREATE, NULL);
    if(!rt) return -1;
    esqlite_backup_type = rt;

    atom_esqlite3 = make_atom(env, "esqlite3");

    if(SQLITE_OK != sqlite3_initialize())
        return -1;

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
    {"start", 0, esqlite_start},
    {"open", 4, esqlite_open},
    {"set_update_hook", 4, set_update_hook},
    {"exec", 4, esqlite_exec},
    {"changes", 3, esqlite_changes},
    {"prepare", 4, esqlite_prepare},
    {"insert", 4, esqlite_insert},
    {"last_insert_rowid", 3, esqlite_last_insert_rowid},
    {"get_autocommit", 3, esqlite_get_autocommit},
    {"multi_step", 5, esqlite_multi_step},
    {"reset", 4, esqlite_reset},
    // TODO: {"esqlite_bind", 3, esqlite_bind_named},
    {"bind", 5, esqlite_bind},
    {"column_names", 4, esqlite_column_names},
    {"column_types", 4, esqlite_column_types},

    {"backup_init", 6, esqlite_backup_init},
    {"backup_step", 5, esqlite_backup_step},
    {"backup_remaining", 4, esqlite_backup_remaining},
    {"backup_pagecount", 4, esqlite_backup_pagecount},
    {"backup_finish", 4, esqlite_backup_finish},

    {"interrupt", 1, esqlite_interrupt, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close", 3, esqlite_close}
};

ERL_NIF_INIT(esqlite3_nif, nif_funcs, on_load, on_reload, on_upgrade, NULL);
