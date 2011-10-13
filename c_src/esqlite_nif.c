/*
 * esqlite -- an erlang sqlite nif.
*/

#include <stdio.h> /* for debugging */
#include <erl_nif.h>

#include "sqlite3.h"

#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite_sqlite3_type = NULL;

static ERL_NIF_TERM _atom_ok;
static ERL_NIF_TERM _atom_error;

typedef struct {
  sqlite3 *db;
} esqlite_sqlite3;

/*
 */
static void descruct_esqlite_sqlite3(ErlNifEnv *env, void *esqlite_db)
{
  /* The destructor should only be called after all the prepared statements are finalized... */
  /* TODO keep references, so this is the case */
  int rc;
  
  rc = sqlite3_close(((esqlite_sqlite3 *) esqlite_db)->db);
  if(rc == SQLITE_BUSY) {
    /* Can I raise exceptions here errors here? */
    fprintf(stderr, "Close failed, still busy\n");
  }
}

/*
 * Open database. Expects utf-8 input
*/
static ERL_NIF_TERM esqlite_open_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    char filename[MAX_PATHNAME];
    sqlite3 *db;
    esqlite_sqlite3 *esqldb;
    ERL_NIF_TERM esqlite_db;
   
    /* TODO ignores the utf-8 stuff for now. nifs only encoding is latin1 */
    if(!enif_get_string(env, argv[0], filename, MAX_PATHNAME, ERL_NIF_LATIN1))
        return enif_make_badarg(env);   

    if(sqlite3_open(filename, &db)) {
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      /* TODO maak een error tuple */
      return enif_make_tuple2(env, _atom_error, _atom_error); /* todo add error message */
    }

    esqldb = enif_alloc_resource(esqlite_sqlite3_type, sizeof(esqlite_sqlite3));
    esqldb->db = db;
    esqlite_db = enif_make_resource(env, esqldb);
    enif_release_resource(esqldb);

    return enif_make_tuple2(env, _atom_ok, esqlite_db);
}

static ERL_NIF_TERM esqlite_prepare_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{

  return enif_make_tuple2(env, _atom_ok, _atom_ok);
}


static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
  ErlNifResourceType *rt = enif_open_resource_type(env, "esqlite", "esqlite_sqlite3_type", 
						   descruct_esqlite_sqlite3, ERL_NIF_RT_CREATE, NULL);
  if(!rt) 
    return -1;

  esqlite_sqlite3_type = rt;

  _atom_ok = enif_make_atom(env, "ok");
  _atom_error = enif_make_atom(env, "error");

  return 0;
}


static ErlNifFunc nif_funcs[] = {
  {"open", 1, esqlite_open_nif},
  {"prepare", 2, esqlite_prepare_nif}
};

ERL_NIF_INIT(esqlite, nif_funcs, on_load, NULL, NULL, NULL);
