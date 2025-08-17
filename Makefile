.PHONY: default
default: install

AWS_ACCOUNT_ID ?= $(shell aws sts get-caller-identity --query Account --output text)
AWS_REGION ?= us-east-2
ECR_REPOSITORY_NAME = sports/nba_stuff
IMAGE_TAG = latest 

ECR_IMAGE_URI = $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPOSITORY_NAME)

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

run:
	@printf "\n" && \
	read -p "Enter script name: " script && \
	printf "\nRunning src/scripts/$${script}.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/scripts/$${script}.py


update_logs:
	@printf "\n" && \
	printf "\nRunning /src/scripts/update_gamelogs.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/scripts/update_gamelogs.py

plays:
	@printf "\n" && \
	printf "\nRunning /src/scripts/new_plays.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/scripts/new_plays.py


revert_logs:
	@printf "\n" && \
	printf "\nRunning /src/scripts/update_gamelogs.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/scripts/revert_gamelogs.py

lines:
	@printf "\n" && \
	printf "\nRunning /src/scripts/lines_analyzer.py\n\n" && \
	source ./.venv/bin/activate && \
	export PYTHONPATH="$(REPO_PATH)" && \
	$(PYTHON) src/scripts/lines_analyzer.py

push: build
	@echo "Authenticating to ECR..."
	# Ensure AWS CLI is configured and has ECR permissions
	aws ecr get-login-password --region $(AWS_REGION) | tr -d "\n" | docker login --username AWS --password-stdin $(ECR_IMAGE_URI)
	@echo "Tagging image for ECR..."
	docker tag $(ECR_REPOSITORY_NAME):$(IMAGE_TAG) $(ECR_IMAGE_URI):$(IMAGE_TAG)
	@echo "Pushing image to ECR: $(ECR_IMAGE_URI):$(IMAGE_TAG)"
	docker push $(ECR_IMAGE_URI):$(IMAGE_TAG)
	@echo "Image pushed to ECR."

deploy: push
	@echo "Updating Lambda function with new image..."
	aws lambda update-function-code \
		--function-name get_player_boxscores \
		--image-uri $(ECR_IMAGE_URI):$(IMAGE_TAG)
	@echo "Lambda function updated."

clean:
	@rm -rf ./.venv