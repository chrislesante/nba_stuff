.PHONY: default
default: install

REPO_PATH=$(CURDIR)/src
PYTHON := python3.10

check_python:
	@if ! command -v $(PYTHON) &> /dev/null; then \
		echo "$(PYTHON) is not installed."; \
		if ! command -v brew &> /dev/null; then \
			echo "Homebrew is not installed. Please install Homebrew from https://brew.sh/."; \
		else \
			echo "Installing Python 3.10 using Homebrew..."; \
			brew update && brew install python@3.10; \
		fi; \
	fi

install: check_python
	@$(PYTHON) -m venv ./.venv && \
	echo 'creating virtual env' && \
	source ./.venv/bin/activate && \
	echo 'Installing required packages' && \
	$(PYTHON) -m pip install -r ./requirements.txt

nba:
	REPO_PATH=$(CURDIR)/src/nba
	@printf "\n" && \
	read -p "Enter script name: " script && \
	printf "\nRunning src/scripts/$${script}.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/nba/scripts/$${script}.py

mlb:
	REPO_PATH=$(CURDIR)/src/mlb
	@printf "\n" && \
	read -p "Enter script name: " script && \
	printf "\nRunning src/scripts/$${script}.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/mlb/scripts/$${script}.py

update_logs:
	REPO_PATH=$(CURDIR)/src/nba
	@printf "\n" && \
	printf "\nRunning /src/nba/scripts/update_gamelogs.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/nba/scripts/update_gamelogs.py

plays:
	REPO_PATH=$(CURDIR)/src/nba
	@printf "\n" && \
	printf "\nRunning /src/scripts/new_plays.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/scripts/new_plays.py


revert_logs:
	REPO_PATH=$(CURDIR)/src/nba
	@printf "\n" && \
	printf "\nRunning /src/nba/scripts/update_gamelogs.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/nba/scripts/revert_gamelogs.py

lines:
	REPO_PATH=$(CURDIR)/src/nba
	@printf "\n" && \
	printf "\nRunning /src/nba/scripts/lines_analyzer.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/nba/scripts/lines_analyzer.py

clean:
	@rm -rf ./.venv