/*
 * Esqlite -- an erlang sqlite nif.
*/

#include <assert.h>
#include <erl_nif.h>

#include <stdio.h> /* for debugging */

#include "queue.h"
#include "sqlite3.h"

#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite_db_type = NULL;

/* database connection context */
typedef struct {
     ErlNifTid tid;
     ErlNifThreadOpts* opts;
  
     sqlite3 *db;
     queue *commands;
     
     int alive;
} esqlite_db;

static ERL_NIF_TERM _atom_ok;
static ERL_NIF_TERM _atom_error;

typedef enum {
     cmd_unknown,
     cmd_open,
     cmd_exec,
     cmd_prepare,
     cmd_step,
     cmd_close,
     cmd_stop
} command_type;

typedef struct {
     command_type type;
     
     ErlNifEnv *env;
     ERL_NIF_TERM ref; 
     ErlNifPid pid;
     ERL_NIF_TERM arg;
} esqlite_command;

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
     return enif_make_tuple2(env, _atom_ok, value);
}

static ERL_NIF_TERM 
make_error_tuple(ErlNifEnv *env, const char *reason) 
{
     return enif_make_tuple2(env, _atom_error, make_atom(env, reason));
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

     return cmd;
}

/*
 *
 */
static void 
descruct_esqlite_db(ErlNifEnv *env, void *arg)
{
     esqlite_db *db = (esqlite_db *) arg;
     esqlite_command *cmd = command_create();
  
     /* Send the stop command 
      */
     cmd->type = cmd_stop;
     queue_push(db->commands, cmd);
     queue_send(db->commands, cmd);
     
     /* Wait for the thread to finish 
      */
     enif_thread_join(db->tid, NULL);
     enif_thread_opts_destroy(db->opts);
     
     /* The thread has finished... now remove the command queue, and close
      * the datbase (if it was still open).
      */
     queue_destroy(db->commands);

     if(db->db)
	  sqlite3_close(db->db);
}

static ERL_NIF_TERM
do_open(ErlNifEnv *env, esqlite_db *db, const ERL_NIF_TERM arg) 
{
     char filename[MAX_PATHNAME];
     unsigned int size;
     int rc;
     ERL_NIF_TERM error;

     if(db->db) 
	  return make_error_tuple(env, "database_already_open");
     
     size = enif_get_string(env, arg, filename, MAX_PATHNAME, ERL_NIF_LATIN1);
     if(size <= 0) 
	  return make_error_tuple(env, "invalid_filename");

     /* Open the database. 
      */
     rc = sqlite3_open(filename, &db->db);
     if(rc != SQLITE_OK) {
	  error = make_error_tuple(env, sqlite3_errmsg(db->db));
	  sqlite3_close(db->db);
	  db->db = NULL;
     
	  return error;
     }
	  
     return _atom_ok;
}

static int 
the_callback(void *a_param, int argc, char **argv, char **column)
{
     /* This only returns null terminated strings... */
     int i;
     for (i = 0; i < argc; i++)
	  fprintf(stderr, "%s,\t", argv[i]);
     fprintf(stderr, "\n");
     return 0;
}


/* 
 * The limit of sqlite3_exec is that it can only return null
 * terminated string values. If you need different datatypes you
 * should use the prepare, bind, step interface.
 */
static ERL_NIF_TERM
do_exec(ErlNifEnv *env, esqlite_db *db, const ERL_NIF_TERM arg)
{
     ErlNifBinary bin;
     int rc;

     if(!db->db) 
	  return make_error_tuple(env, "database_not_open");

     /* Get the query as a binary -- and the end of string -- */
     enif_inspect_iolist_as_binary(env, arg, &bin);
     
     rc = sqlite3_exec(db->db, (char *) bin.data, the_callback, NULL, NULL);

     /* TODO: check rc*/

     return _atom_ok;
}

static ERL_NIF_TERM
do_prepare(ErlNifEnv *env, esqlite_db *conn, const ERL_NIF_TERM arg)
{
     ErlNifBinary bin;
     int rc;
     
     if(!conn->db)
	  return make_error_tuple(env, "database_not_open");

     /* Get the query as binary -- and the included end of string -- */
     enif_inspect_iolist_as_binary(env, arg, &bin);

     fprintf(stderr, "do_prepare\n");

     /* return the prepared statement. */

     

     return _atom_ok;
}

static ERL_NIF_TERM
do_step(ErlNifEnv *env, esqlite_db *db, const ERL_NIF_TERM arg)
{
     fprintf(stderr, "do_prepare\n");
     return _atom_ok;
}

static ERL_NIF_TERM
do_close(ErlNifEnv *env, esqlite_db *conn, const ERL_NIF_TERM arg)
{
     int rc;

     if(!conn->db)
	  return make_error_tuple(env, "database_not_open");

     rc = sqlite3_close(conn->db);
     if(rc != SQLITE_OK) 
	  return make_error_tuple(env, sqlite3_errmsg(conn->db));

     conn->db = NULL;
     return _atom_ok;
}

static ERL_NIF_TERM
evaluate_command(ErlNifEnv *env, command_type type, esqlite_db *db, const ERL_NIF_TERM arg)
{
     switch(type) {
     case cmd_open:
	  return do_open(env, db, arg);
     case cmd_exec:
	  return do_exec(env, db, arg);
     case cmd_prepare:
	  return do_prepare(env, db, arg);
     case cmd_step:
	  return do_step(env, db, arg);
     case cmd_close:
	  return do_close(env, db, arg);
     default:
	  return make_error_tuple(env, "invalid_command");
     }
}

static void *
esqlite_db_run(void *arg)
{
     esqlite_db *db = (esqlite_db *) arg;
     esqlite_command *cmd;
     int continue_running = 1;
     
     db->alive = 1;

     while(continue_running) {
	  cmd = queue_pop(db->commands);
    
	  if(cmd->type == cmd_stop) {
	       continue_running = 0;
	  } else {
	       enif_send(NULL, &cmd->pid, cmd->env, 
			 enif_make_tuple2(cmd->env, cmd->ref, 
					  evaluate_command(cmd->env, cmd->type, db, cmd->arg)));
	  }
    
	  command_destroy(cmd);    
     }
  
     db->alive = 0;
     return NULL;
}

/* 
 * Start the processing thread
 */
static ERL_NIF_TERM 
start_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
     esqlite_db *esqldb;
     ERL_NIF_TERM db;

     /* Initialize the resource */
     esqldb = enif_alloc_resource(esqlite_db_type, sizeof(esqlite_db));
     esqldb->db = NULL;

     /* Create command queue */
     esqldb->commands = queue_create();
     if(!esqldb->commands) {
	  enif_release_resource(esqldb);
	  return make_error_tuple(env, "command_queue_create_failed");
     }

     /* Start command processing thread */
     esqldb->opts = enif_thread_opts_create("esqldb_thread_opts");
     if(enif_thread_create("", &esqldb->tid, esqlite_db_run, esqldb, esqldb->opts) != 0) {
	  enif_release_resource(esqldb);
	  return make_error_tuple(env, "thread_create_failed");
     }

     db = enif_make_resource(env, esqldb);
     enif_release_resource(esqldb);
  
     return make_ok_tuple(env, db);
}

/* 
 * Open the database
 */
static ERL_NIF_TERM
esqlite_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
     esqlite_db *db;
     esqlite_command *cmd = NULL;
     ErlNifPid pid;
     
     if(argc != 4) 
	  return enif_make_badarg(env);
     
     if(!enif_get_resource(env, argv[0], esqlite_db_type, (void **) &db))
	  return enif_make_badarg(env);
     
     if(!enif_is_ref(env, argv[1])) 
	  return make_error_tuple(env, "invalid_ref");

     if(!enif_get_local_pid(env, argv[2], &pid)) 
	  return make_error_tuple(env, "invalid_pid");

     /* Note, no check is made for the type of the argument */

     cmd = command_create();
     if(!cmd) 
	  return make_error_tuple(env, "command_create_failed");

     /* command */
     cmd->type = cmd_open;
     cmd->ref = enif_make_copy(cmd->env, argv[1]);
     cmd->pid = pid;
     cmd->arg = enif_make_copy(cmd->env, argv[3]);

     if(!queue_push(db->commands, cmd)) 
	  return make_error_tuple(env, "command_push_failed");
  
     return _atom_ok;
}

/*
 * Execute the sql statement
 */
static ERL_NIF_TERM 
esqlite_exec_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
     esqlite_db *db;
     esqlite_command *cmd = NULL;
     ErlNifPid pid;
     
     if(argc != 4) 
	  return enif_make_badarg(env);
     
     if(!enif_get_resource(env, argv[0], esqlite_db_type, (void **) &db))
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

     if(!queue_push(db->commands, cmd)) 
	  return make_error_tuple(env, "command_push_failed");
  
     return _atom_ok;  
}

/*
 * Close the database
 */
static ERL_NIF_TERM
esqlite_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
     return _atom_ok;
}

/*
 * Load the nif. Initialize some stuff and such
 */
static int 
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
     ErlNifResourceType *rt = enif_open_resource_type(env, "esqlite", "esqlite_sqlite3_db", 
						      descruct_esqlite_db, ERL_NIF_RT_CREATE, NULL);
     if(!rt) 
	  return -1;

     esqlite_db_type = rt;

     _atom_ok = make_atom(env, "ok");
     _atom_error = make_atom(env, "error");

     return 0;
}

static ErlNifFunc nif_funcs[] = {
     {"esqlite_start", 0, start_nif},
     {"esqlite_open", 4, esqlite_open_nif},
     {"esqlite_exec", 4, esqlite_exec_nif},
     {"esqlite_prepare", 4, esqlite_prepare_nif},
     {"esqlite_bind", 3, esqlite_bind_named},
     {"esqlite_bind", 2, esqlite_bind},
     {"esqlite_close", 3, esqlite_close_nif}
};

ERL_NIF_INIT(esqlite, nif_funcs, on_load, NULL, NULL, NULL);
