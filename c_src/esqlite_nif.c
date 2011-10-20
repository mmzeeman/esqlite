/*
 * Esqlite -- an erlang sqlite nif.
*/

#include <stdio.h> /* for debugging */
#include <assert.h>
#include <erl_nif.h>

#include "queue.h"
#include "sqlite3.h"

#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite_sqlite3_type = NULL;

static ERL_NIF_TERM _atom_ok;
static ERL_NIF_TERM _atom_error;

/* database connection context */
typedef struct {
  ErlNifTid tid;
  ErlNifThreadOpts* opts;
  
  sqlite3 *db;
  queue *commands;

  int alive;
} esqlite_db;

typedef enum {
  cmd_unknown,
  cmd_open,
  cmd_exec,
  cmd_close,
  cmd_stop
} command_type;

typedef struct {
  command_type type;

  ErlNifEnv *env;
  ERL_NIF_TERM ref; 
  ErlNifPid pid;

  /* Args */

} esqlite_command;

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

  return cmd;
}

/*
 */
static void 
descruct_esqlite_db(ErlNifEnv *env, void *arg)
{
  esqlite_db *db = (esqlite_db *) arg;
  esqlite_command *cmd = command_create();

  /* send the stop command */
  cmd->type = cmd_stop;
  queue_push(db->commands, cmd);
  queue_send(db->commands, cmd);

  /* wait for the thread to finish */
  enif_thread_join(db->tid, NULL);
  enif_thread_opts_destroy(db->opts);
}

static void *
esqlite_db_run(void *arg)
{
  esqlite_db *db = (esqlite_db *) arg;
  esqlite_command *cmd;

  db->alive = 1;

  while(1) {
    cmd = queue_pop(db->commands);

    /* We are stopping... */
    if(cmd_stop == cmd->type) {
      command_destroy(cmd);
      break;
    }

    /* Evaluate the command */
    switch(cmd->type) {
    cmd_open:
      /* do open */
      break;
    cmd_exec:
      /* do exec */
      break;
    cmd_close:
      /* do close */
      break;
    default:
      assert(0 && "Invalid command");
    }

    enif_send(NULL, &(cmd->pid), cmd->env, _atom_ok);
    command_destroy(cmd);
  }

  db->alive = 0;
  return NULL;
}

/*
 * Open database. Expects utf-8 input
 *
 * Note the database is opened in a thread. New commands are send to
 * the thread and when it finishes the result is send back. The reason
 * for this is that we don't want to block the erlang scheduler of the
 * calling function.
 */
static ERL_NIF_TERM 
start_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  esqlite_db *esqldb;
  ERL_NIF_TERM esqlite_db;

  /* initialize the resource */
  esqldb = enif_alloc_resource(esqlite_sqlite3_type, sizeof(esqlite_db));
  esqldb->db = NULL;

  /* Start the command processing thread */
  esqldb->opts = enif_thread_opts_create("esqldb_thread_opts");
  if(enif_thread_create("", &esqldb->tid, esqlite_db_run, esqldb, esqldb->opts) != 0) {
    enif_release_resource(esqldb);
    return enif_make_tuple2(env, _atom_error, _atom_ok);
  }

  /* We got the resource... now return it */
  esqlite_db = enif_make_resource(env, esqldb);
  enif_release_resource(esqldb);
  return enif_make_tuple2(env, _atom_ok, esqlite_db);
}

static ERL_NIF_TERM
esqlite_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  return _atom_ok;
}

static ERL_NIF_TERM 
esqlite_exec_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  return _atom_ok;
}

static ERL_NIF_TERM
esqlite_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
  return _atom_ok;
}

static int 
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
  ErlNifResourceType *rt = enif_open_resource_type(env, "esqlite", "esqlite_sqlite3_type", 
						   descruct_esqlite_db, ERL_NIF_RT_CREATE, NULL);
  if(!rt) 
    return -1;

  esqlite_sqlite3_type = rt;

  _atom_ok = enif_make_atom(env, "ok");
  _atom_error = enif_make_atom(env, "error");

  return 0;
}


static ErlNifFunc nif_funcs[] = {
  {"esqlite_start", 0, start_nif},
  {"esqlite_open", 2, esqlite_open_nif},
  {"esqlite_exec", 4, esqlite_exec_nif}, 
  {"esqlite_close", 3, esqlite_close_nif}
};

ERL_NIF_INIT(esqlite, nif_funcs, on_load, NULL, NULL, NULL);
