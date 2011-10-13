/*
 * esqlite -- an erlang sqlite nif.
*/

#include <erl_nif.h>

#include "sqlite3.h"

#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h */

/*
 * Open database. Expects utf-8 input
*/
static ERL_NIF_TERM eisqlite_open_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    int rc;
    char filename[MAX_PATHNAME];
    sqlite3
   
    /* TODO ignore the utf-8 stuff for now */
    if(!enif_get_string(env, argv[0], filename, MAX_PATHNAME, ERL_NIF_LATIN1))
        return enif_make_badarg(env);   


    rc = sqlite3_open(filename, &db);
    if( rc ) {
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      /* maak een error tuple */
    }
    
}


static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
  return 0;
}


static ErlNifFunc nif_funcs[] = {
};

ERL_NIF_INIT(esqlite, nif_funcs, on_load, NULL, NULL, NULL);
