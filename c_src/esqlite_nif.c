/*
 * esqlite -- an erlang sqlite nif.
*/

#include <erl_nif.h>

#include "sqlite3.h"


static int on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
  return 0;
}


static ErlNifFunc nif_funcs[] = {
};

ERL_NIF_INIT(esqlite, nif_funcs, on_load, NULL, NULL, NULL);
