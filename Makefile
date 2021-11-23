REBAR = ./rebar3

all: compile

$(REBAR):
	wget https://s3.amazonaws.com/rebar3/rebar3 -O $(REBAR)
	chmod +x $(REBAR)

compile: $(REBAR)
	$(REBAR) compile

xref: $(REBAR)
	$(REBAR) xref

format: $(REBAR)
	@$(REBAR) format

clean: distclean

distclean:
	@rm -rf _build cover deps logs log data
	@rm -rf rebar.lock compile_commands.json
	@rm -rf rebar3.crashdump