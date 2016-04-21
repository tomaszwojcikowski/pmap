REBAR = $(shell pwd)/rebar

all: compile

compile:
	$(REBAR) compile

update:
	$(REBAR) update

test: compile
	$(REBAR) eunit

run:
	$(REBAR) shell