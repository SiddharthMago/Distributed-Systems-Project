# Local settings belong in .env. Override any value with: make peer ID=peer2 PORT=8102
ENV_FILE ?= .env
-include $(ENV_FILE)

PYTHON ?= python3.12
VENV ?= .venv
ID ?= peer1
# IP is the address other PCs can reach. BIND is the local listening interface.
IP ?= 127.0.0.1
BIND ?= 0.0.0.0
PORT ?= 8101
DIRECTORY_IP ?= 127.0.0.1
DIRECTORY_PORT ?= 8080
DIRECTORY_URL ?= http://$(DIRECTORY_IP):$(DIRECTORY_PORT)
ADVERTISE ?= http://$(IP):$(PORT)
STATE ?= run/manual/$(ID)
DB ?= run/manual/directory.sqlite3
RATE ?= 0.5
VERBOSE ?= 1
INTERACTIVE ?= 1
CORRUPT_INDEX ?=
# Append diagnostics here. Set LOG= to display them in the terminal instead.
LOG ?= run/manual/$(ID).log

SHELL := /bin/sh
.DEFAULT_GOAL := help
.PHONY: help setup check-env directory peer test experiment
export PYTHON VENV ID IP BIND PORT DIRECTORY_IP DIRECTORY_PORT DIRECTORY_URL
export ADVERTISE STATE DB RATE VERBOSE INTERACTIVE CORRUPT_INDEX LOG

help:
	@printf '%s\n' \
	  'make setup                         Create the virtual environment and install dependencies' \
	  'make directory                     Start the persistent directory' \
	  'make peer                          Start an interactive peer using .env or defaults' \
	  'make peer ID=peer2 PORT=8102        Override settings for another peer' \
	  'make peer CORRUPT_INDEX=0           Start a deliberately corrupt source for testing' \
	  'make peer LOG=                     Show diagnostics in the terminal' \
	  'make test                          Run the automated tests' \
	  'make experiment                    Run the local six-peer experiment' \
	  '' \
	  'Copy .env.example to .env for per-PC settings. Command-line values take precedence.' \
	  'Use a reachable IP for IP and DIRECTORY_IP. BIND=0.0.0.0 listens on all interfaces.'

setup:
	@"$$PYTHON" -m venv "$$VENV"
	@"$$VENV/bin/python" -m pip install -r requirements-dev.txt

check-env:
	@test -x "$$VENV/bin/python" || { printf '%s\n' 'Python environment missing. Run make setup first.' >&2; exit 1; }

directory: check-env
	@exec "$$VENV/bin/python" -m napster directory \
	  --bind "$$BIND" --port "$$DIRECTORY_PORT" --db "$$DB"

peer: check-env
	@set -eu; \
	  case "$$IP" in 0.0.0.0|::) \
	    printf '%s\n' 'IP must be a reachable peer address. Use BIND=0.0.0.0 for wildcard listening.' >&2; exit 1;; \
	  esac; \
	  case "$$VERBOSE:$$INTERACTIVE" in 0:0|0:1|1:0|1:1) ;; \
	    *) printf '%s\n' 'VERBOSE and INTERACTIVE must each be 0 or 1.' >&2; exit 1;; \
	  esac; \
	  set -- "$$VENV/bin/python" -m napster; \
	  if [ "$$VERBOSE" = 1 ]; then set -- "$$@" --verbose; fi; \
	  set -- "$$@" peer --id "$$ID" --state "$$STATE" \
	    --bind "$$BIND" --port "$$PORT" --advertise "$$ADVERTISE" \
	    --directory "$$DIRECTORY_URL" --rate-mib "$$RATE"; \
	  if [ "$$INTERACTIVE" = 1 ]; then set -- "$$@" --interactive; fi; \
	  if [ -n "$$CORRUPT_INDEX" ]; then set -- "$$@" --corrupt-index "$$CORRUPT_INDEX"; fi; \
	  if [ -n "$$LOG" ]; then \
	    mkdir -p "$$(dirname "$$LOG")"; \
	    printf 'Diagnostics append to %s\n' "$$LOG"; \
	    exec "$$@" 2>> "$$LOG"; \
	  fi; \
	  exec "$$@"

test: check-env
	@exec "$$VENV/bin/python" -m pytest -q

experiment: check-env
	@exec "$$VENV/bin/python" -m napster.experiment
