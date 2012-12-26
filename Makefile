PROJECT = esqlite
REBAR = ./rebar
DIALYZER = dialyzer

all: compile

rebar:
	wget https://github.com/downloads/basho/rebar/rebar -O $(REBAR)
	chmod u+x $(REBAR)

compile: rebar
	$(REBAR) compile

test: compile
	$(REBAR) eunit

clean: rebar
	$(REBAR) clean

distclean: 
	rm $(REBAR)

# dializer 

build-plt:
	@$(DIALYZER) --build_plt --output_plt .$(PROJECT).plt \
		--apps kernel stdlib 

dialyze:
	@$(DIALYZER) --src src --plt .$(PROJECT).plt --no_native \
		-Werror_handling -Wrace_conditions -Wunmatched_returns -Wunderspecs

