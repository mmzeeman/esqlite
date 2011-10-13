/*
 * esqlite -- an erlang sqlite nif.
*/

#include <erl_nif.h>

#include "sqlite3.h"

static ErlNifFunc nif_funcs[] = {
};

ERL_NIF_INIT(esqlite, nif_funcs, on_load, NULL, NULL, NULL);
