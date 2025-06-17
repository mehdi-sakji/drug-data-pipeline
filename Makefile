SHELL = /bin/bash
.PHONY: venv update test coverage

PYTHON_VERSION=3.11

venv:	## Create virtualenv by Conda
	. ./ensure_conda && conda create -yq -p venv python=$(PYTHON_VERSION)
	. ./activate_venv && \
			python -m pip install -U -q -c constraints.txt pip && \
			pip install -U -q -c constraints.txt -e . -r requirements.txt
	. ./activate_venv

update: venv  ## Update dependencies
	. ./activate_venv && pip install -U -q -c constraints.txt -e . -r requirements.txt

test:  ## Run tests
	. ./activate_venv && pytest $(PYTEST_OPTIONS)

coverage: venv  ## Run tests and compute test coverage
	. ./activate_venv && \
		coverage run -m pytest --durations=0 $(PYTEST_OPTIONS) && \
		coverage report