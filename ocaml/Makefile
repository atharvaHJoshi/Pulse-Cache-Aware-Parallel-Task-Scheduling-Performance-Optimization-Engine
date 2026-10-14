# Standalone build for the OCaml reference model.
#
# The model uses only the OCaml standard library, so either the plain compiler
# (ocamlopt) or dune works.  `make` prefers dune when present and falls back to
# ocamlopt otherwise.

OCAMLOPT ?= ocamlopt

.PHONY: all clean

all: pulse_ref

pulse_ref: pulse_ref.ml
	@if command -v dune >/dev/null 2>&1; then \
		dune build ./pulse_ref.exe && cp -f _build/default/pulse_ref.exe pulse_ref; \
	else \
		$(OCAMLOPT) -o pulse_ref pulse_ref.ml; \
	fi

clean:
	@rm -f pulse_ref pulse_ref.cmi pulse_ref.cmx pulse_ref.o
	@if command -v dune >/dev/null 2>&1; then dune clean; fi
