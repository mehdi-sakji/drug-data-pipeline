SHELL = /bin/bash
.PHONY: venv update test coverage

PYTHON_VERSION=3.11
PROJECT_NAME=drug-data-pipeline
FLOW_PATH=src/pipeline/dag.py:main_flow
DEPLOYMENT_NAME=drug-data-deployment
POOL_NAME=drug-data-agent-pool

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

run-adhoc:
	. ./activate_venv && python src/adhoc/main.py $(MATCHES_PATH)

init:
	. ./activate_venv && prefect init --name $(PROJECT_NAME)

build:
	. ./activate_venv && prefect deployment build $(FLOW_PATH) --name $(DEPLOYMENT_NAME) --pool $(POOL_NAME) --params '$(PARAMS)'

apply:
	. ./activate_venv && prefect deployment apply ./deployments/$(DEPLOYMENT_NAME).yaml

run:
	. ./activate_venv && prefect deployment run main-flow/$(DEPLOYMENT_NAME)

worker:
	. ./activate_venv && prefect worker start --pool drug-data-agent-pool

deploy-and-run:
	$(VENV_ACTIVATE) && \
	prefect deployment build $(FLOW_PATH) --name $(DEPLOYMENT_NAME) --pool $(POOL_NAME) --params '$(PARAMS)' && \
	prefect deployment apply ./deployments/$(DEPLOYMENT_NAME).yaml && \
	prefect deployment run main-flow/$(DEPLOYMENT_NAME)