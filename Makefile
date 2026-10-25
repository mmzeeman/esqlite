PROJECT = esqlite
DIALYZER = dialyzer

ERL       ?= erl
REBAR3 := $(shell which rebar3 2>/dev/null || echo ./rebar3)
REBAR3_VERSION := 3.27.3
REBAR3_URL := https://github.com/erlang/rebar3/releases/download/$(REBAR3_VERSION)/rebar3
REBAR_SHA256 := f1d87d78decfc56b87d971c75ee6355698ec0c49fadd57f2c056043b7ca291b5
REBAR_STAMP := ./.rebar3-$(REBAR_SHA256)

all: compile

$(REBAR_STAMP):
	rm -f ./.rebar-*
	touch $@
	rm -f $(REBAR3)

$(REBAR3): $(REBAR_STAMP)
	./tools/download.escript "$(REBAR3_URL)" "$(REBAR3)" --sha256 "$(REBAR_SHA256)" --executable $(PROXY_ARGS)

compile: $(REBAR3)
	$(REBAR3) compile

test: compile
	$(REBAR3) eunit

clean: $(REBAR3)
	$(REBAR3) clean

distclean:
	rm $(REBAR3)

# dialyzer

build-plt:
	@$(DIALYZER) --build_plt --output_plt .$(PROJECT).plt \
		--apps kernel stdlib

dialyze:
	@$(DIALYZER) --src src --plt .$(PROJECT).plt --no_native \
		-Werror_handling -Wrace_conditions -Wunmatched_returns -Wunderspecs

