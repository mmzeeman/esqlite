REBAR=./rebar

all: compile

rebar:
	wget https://bitbucket.org/basho/rebar/downloads/rebar -O $(REBAR)
	chmod u+x $(REBAR)

compile: rebar
	$(REBAR) compile

test: compile
	$(REBAR) eunit

clean: rebar
	$(REBAR) clean

distclean: 
	rm $(REBAR)
