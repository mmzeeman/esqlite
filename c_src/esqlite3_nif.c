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
#define MAX_PATHNAME 512            /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite3_type = NULL;
static ErlNifResourceType *esqlite3_stmt_type = NULL;
static ErlNifResourceType *esqlite3_backup_type = NULL;

/* Database connection context */
typedef struct {
    sqlite3 *db;

    ErlNifPid update_hook_pid;
} esqlite3;

/* Prepared statement */
typedef struct {
    esqlite3 *connection;

    sqlite3_stmt *statement;
    int column_count;
} esqlite3_stmt;

/* Data associated with an ongoing backup */
typedef struct {
    esqlite3 *source;
    esqlite3 *destination;

    sqlite3_backup *backup;
} esqlite3_backup;


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
make_sqlite3_error_tuple(ErlNifEnv *env, int error_code) {
    return enif_make_tuple2(env, make_atom(env, "error"), enif_make_int(env, error_code));
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

    stmt->column_count = 0;

    if(stmt->connection) {
        enif_release_resource(stmt->connection);
        stmt->connection = NULL;
    }
}

static void
destruct_esqlite3_backup(ErlNifEnv *env, void *arg) 
{
    esqlite3_backup *backup = (esqlite3_backup *) arg;

    if(backup->backup) {
        sqlite3_backup_finish(backup->backup);
    }
    backup->backup = NULL;

    if(backup->destination) {
        enif_release_resource(backup->destination);
        backup->destination = NULL;
    }

    if(backup->source) {
        enif_release_resource(backup->source);
        backup->source = NULL;
    }
}

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

    if(conn->db == NULL) {
        return make_atom(env, "ok");
    }

    /* 
     * close_v2 is not guaranteed to close the connection. There could be
     * an open transaction. Rollback this transaction first, before trying 
     * to close the connection.
     */
    if(!sqlite3_get_autocommit(conn->db)) {
        rc = sqlite3_exec(conn->db, "ROLLBACK;", NULL, NULL, NULL);
        if (rc != SQLITE_OK) {
            return make_sqlite3_error_tuple(env, rc);
        }
    }

    rc = sqlite3_close_v2(conn->db);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    conn->db = NULL;
    return make_atom(env, "ok");
}

/*
 * Return a description of the last occurred error.
 */
static ERL_NIF_TERM
esqlite_error_info(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    if(conn->db == NULL) {
        enif_make_badarg(env);
    }

    int code = sqlite3_errcode(conn->db);
    int extended_code = sqlite3_extended_errcode(conn->db);
    const char *errstr = sqlite3_errstr(extended_code);
    const char *errmsg = sqlite3_errmsg(conn->db);

    ERL_NIF_TERM info = enif_make_new_map(env);
    enif_make_map_put(env, info, make_atom(env, "errcode"), enif_make_int(env, code), &info);
    enif_make_map_put(env, info, make_atom(env, "extended_errcode"), enif_make_int(env, extended_code), &info);
    enif_make_map_put(env, info, make_atom(env, "errstr"), make_binary(env, errstr, strlen(errstr)), &info);
    enif_make_map_put(env, info, make_atom(env, "errmsg"), make_binary(env, errmsg, strlen(errmsg)), &info);
    enif_make_map_put(env, info, make_atom(env, "error_offset"), enif_make_int(env, sqlite3_error_offset(conn->db)), &info);

    return info;
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

    enif_free_env(msg_env);
}


static ERL_NIF_TERM
esqlite_set_update_hook(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn)) {
        return enif_make_badarg(env);
    }

    if(!conn->db) {
        return enif_raise_exception(env, make_atom(env, "closed"));
    }

    if(enif_is_atom(env, argv[1])) {
        /* Reset the hook when an atom is passed */
        sqlite3_update_hook(conn->db, NULL, NULL);
    } else {
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
    /* Keep a reference to the connection to prevent it from being garbage collected
     * before the statement.
     */
    enif_keep_resource((void *) conn);
    stmt->connection = conn;

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

    if(!enif_inspect_iolist_as_binary(env, argv[2], &text)) {
        return enif_make_badarg(env);
    }

    /* 
     * Don't do any checks on the input data, sqlite handes all kinds of input. It is
     * garbage-in, garbage-out.
     * 
     */

    int rc = sqlite3_bind_text64(stmt->statement, index, (char *) text.data, text.size, SQLITE_TRANSIENT, SQLITE_UTF8);
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

    if(!enif_inspect_iolist_as_binary(env, argv[2], &blob)) {
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
esqlite_bind_parameter_index(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_stmt *stmt;
    ErlNifBinary bin;
    int index;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_stmt_type, (void **) &stmt)) {
        return enif_make_badarg(env);
    }

    if(!stmt->statement) {
        return enif_raise_exception(env, make_atom(env, "no_prepared_statement"));   
    }

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &bin)) {
        return enif_make_badarg(env);
    }

    index = sqlite3_bind_parameter_index(stmt->statement, bin.data);
    if(index == 0) {
        return enif_make_atom(env, "error");
    }

    return make_ok_tuple(env, enif_make_int(env, index));
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
            /*
             * Automatically reset the statement after a done so
             * column_names will work after the statement is done.
             *
             * Not resetting the statement can lead to vm crashes.
             */
            sqlite3_reset(stmt->statement);
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

static ERL_NIF_TERM
esqlite_backup_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *destination;
    ErlNifBinary destination_name;
    esqlite3 *source;
    ErlNifBinary source_name;
    ERL_NIF_TERM eos = enif_make_int(env, 0);

    if(argc != 4) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &destination)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[1], eos), &destination_name)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[2], esqlite3_type, (void **) &source)) {
        return enif_make_badarg(env);
    }

    if(!enif_inspect_iolist_as_binary(env, enif_make_list2(env, argv[3], eos), &source_name)) {
        return enif_make_badarg(env);
    }

    sqlite3_backup *backup = sqlite3_backup_init(destination->db, (const char *) destination_name.data, source->db, (const char *) source_name.data);
    if(backup == NULL) {
        return make_sqlite3_error_tuple(env, sqlite3_errcode(destination->db));
    }

    esqlite3_backup *ebackup = enif_alloc_resource(esqlite3_backup_type, sizeof(esqlite3_backup));
    if(!ebackup) {
        (void) sqlite3_backup_finish(backup);
        return enif_raise_exception(env, make_atom(env, "no_memory"));   
    }

    ebackup->backup = backup;

    /**
     * Keep references to both database connections to prevent
     * them from being garbage collected during the operation.
     */
    enif_keep_resource((void *)destination);
    ebackup->destination = destination;
    enif_keep_resource((void *)source);
    ebackup->source = source;

    ERL_NIF_TERM erl_backup_term = enif_make_resource(env, ebackup);
    enif_release_resource(ebackup);

    return make_ok_tuple(env, erl_backup_term);
}

/*
 * Get the remaining pagecount of the backup.
 */
static ERL_NIF_TERM
esqlite_backup_remaining(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_backup *backup;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_backup_type, (void **) &backup)) {
        return enif_make_badarg(env);
    }

    sqlite3_int64 remaining = sqlite3_backup_remaining(backup->backup);

    return enif_make_int64(env, remaining);
}

/*
 * Get the total pagecount of the backup
 */
static ERL_NIF_TERM
esqlite_backup_pagecount(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_backup *backup;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_backup_type, (void **) &backup)) {
        return enif_make_badarg(env);
    }

    sqlite3_int64 pagecount = sqlite3_backup_pagecount(backup->backup);

    return enif_make_int64(env, pagecount);
}

static ERL_NIF_TERM
esqlite_backup_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_backup *backup;
    int n_page;

    if(argc != 2) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_backup_type, (void **) &backup)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[1], &n_page)) {
        return enif_make_badarg(env);
    }

    int rc = sqlite3_backup_step(backup->backup, n_page);
    if(rc == SQLITE_DONE) {
        return make_atom(env, "$done");
    }  

    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

/*
 * Explicitly finish the backup.
 */
static ERL_NIF_TERM
esqlite_backup_finish(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3_backup *backup;

    if(argc != 1) {
        return enif_make_badarg(env);
    }

    if(!enif_get_resource(env, argv[0], esqlite3_backup_type, (void **) &backup)) {
        return enif_make_badarg(env);
    }

    int rc = SQLITE_OK;
    if(backup->backup) {
        rc = sqlite3_backup_finish(backup->backup);
        backup->backup = NULL;
    }

    if(backup->source) {
        enif_release_resource(backup->source);
        backup->source = NULL;
    }

    if(backup->destination) {
        enif_release_resource(backup->destination);
        backup->destination = NULL;
    }

    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    return make_atom(env, "ok");
}

/*
 * Interrupt any currently active query.
 */
static ERL_NIF_TERM
esqlite_interrupt(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;
    if(db->db == NULL) {
        return enif_raise_exception(env, make_atom(env, "closed"));
    }

    sqlite3_interrupt(db->db);
    return enif_make_atom(env, "ok");
}

static ERL_NIF_TERM
esqlite_get_autocommit(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;

    if(db->db == NULL) {
        return enif_raise_exception(env, make_atom(env, "closed"));
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

    if(argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;

    if(db->db == NULL) {
        return enif_raise_exception(env, make_atom(env, "closed"));
    }

    sqlite3_int64 last_rowid = sqlite3_last_insert_rowid(db->db);
    return enif_make_int64(env, last_rowid);
}

static ERL_NIF_TERM
esqlite_changes(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite3 *conn;

    if(argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite3_type, (void **) &conn))
        return enif_make_badarg(env);

    esqlite3 *db = (esqlite3 *) conn;

    if(db->db == NULL) {
        return enif_raise_exception(env, make_atom(env, "closed"));
    }

    sqlite3_int64 changes = sqlite3_changes64(db->db);
    return enif_make_int64(env, changes);
}

static ERL_NIF_TERM
esqlite_memory_stats(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    int highwater_reset_flag = 0;

    if(argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_int(env, argv[0], &highwater_reset_flag)) {
        return enif_make_badarg(env);
    }

    int used = sqlite3_memory_used();
    int highwater = sqlite3_memory_highwater(highwater_reset_flag);

    ERL_NIF_TERM stats = enif_make_new_map(env);
    enif_make_map_put(env, stats, make_atom(env, "used"), enif_make_int64(env, used), &stats);
    enif_make_map_put(env, stats, make_atom(env, "highwater"), enif_make_int64(env, highwater), &stats);
    
    return stats;
}

static ERL_NIF_TERM
esqlite_status(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    int op = 0;
    int highwater_reset_flag = 0;

    if(argc != 2)
        return enif_make_badarg(env);

    if(!enif_get_int(env, argv[0], &op)) {
        return enif_make_badarg(env);
    }

    if(!enif_get_int(env, argv[0], &highwater_reset_flag)) {
        return enif_make_badarg(env);
    }

    sqlite3_int64 used;
    sqlite3_int64 highwater;

    int rc = sqlite3_status64(op, &used, &highwater, highwater_reset_flag);
    if(rc != SQLITE_OK) {
        return make_sqlite3_error_tuple(env, rc);
    }

    ERL_NIF_TERM stats = enif_make_new_map(env);
    enif_make_map_put(env, stats, make_atom(env, "used"), enif_make_int64(env, used), &stats);
    enif_make_map_put(env, stats, make_atom(env, "highwater"), enif_make_int64(env, highwater), &stats);
    
    return stats;
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

    {"error_info", 1, esqlite_error_info},

    /*
     * Other interesting additions... trace.
     * wal_hook triggered after every commit.
     * also interesting commit and rollback_hooks
     */
    {"set_update_hook", 2, esqlite_set_update_hook},

    {"exec", 2, esqlite_exec, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"prepare", 3, esqlite_prepare},

    {"column_names", 1, esqlite_column_names},
    {"column_decltypes", 1, esqlite_column_decltypes},

    {"bind_int", 3, esqlite_bind_int},
    {"bind_int64", 3, esqlite_bind_int64},
    {"bind_double", 3, esqlite_bind_double},
    {"bind_text", 3, esqlite_bind_text},
    {"bind_blob", 3, esqlite_bind_blob},
    {"bind_null", 2, esqlite_bind_null},
    {"bind_parameter_index", 2, esqlite_bind_parameter_index},

    {"step", 1, esqlite_step, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"reset", 1, esqlite_reset},

    {"interrupt", 1, esqlite_interrupt, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"last_insert_rowid", 1, esqlite_last_insert_rowid},
    {"get_autocommit", 1, esqlite_get_autocommit},
    {"changes", 1, esqlite_changes},

    {"backup_init", 4, esqlite_backup_init, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"backup_remaining", 1, esqlite_backup_remaining},
    {"backup_pagecount", 1, esqlite_backup_pagecount},
    {"backup_step", 2, esqlite_backup_step, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"backup_finish", 1, esqlite_backup_finish, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"memory_stats", 1, esqlite_memory_stats},
    {"status", 2, esqlite_status}
};

ERL_NIF_INIT(esqlite3_nif, nif_funcs, on_load, on_reload, on_upgrade, NULL);
