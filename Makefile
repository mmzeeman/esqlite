PROJECT = esqlite
DIALYZER = dialyzer

REBAR3 := $(shell which rebar3 2>/dev/null || echo ./rebar3)
REBAR3_VERSION := 3.10.0
REBAR3_URL := https://github.com/erlang/rebar3/releases/download/$(REBAR3_VERSION)/rebar3

all: compile

./rebar3:
	wget $(REBAR3_URL)
	chmod +x ./rebar3

compile: rebar3
	$(REBAR3) compile

test: compile
	$(REBAR3) eunit

clean: rebar3
	$(REBAR3) clean

distclean:
	rm $(REBAR3)

# dializer

build-plt:
	@$(DIALYZER) --build_plt --output_plt .$(PROJECT).plt \
		--apps kernel stdlib

dialyze:
	@$(DIALYZER) --src src --plt .$(PROJECT).plt --no_native \
		-Werror_handling -Wrace_conditions -Wunmatched_returns -Wunderspecs

