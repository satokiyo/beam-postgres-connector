.DEFAULT_GOAL := help
.PHONY: test tox lint format isort type_check static_test

test: ## test ## make test
	@docker-compose -f tests/docker-compose.yml up -d; \
	sleep 2; \
	poetry run python -m pytest; \
	docker-compose -f tests/docker-compose.yml down --volume;

tox: ## test in multiple python versions ## make tox
	@poetry run python -m tox

lint: ## lint ## make lint
	@poetry run pflake8 src/ tests/

format: ## format ## make format
	@poetry run black --diff --color .
	@poetry run black .

isort: ## isort ## make isort
	@poetry run isort .

type_check: ## type_check ## make type_check
	@poetry run mypy .

static_test: ## static_test ## make static_test
	@echo start type_check
	-@$(MAKE) type_check
	@echo start isort
	-@$(MAKE) isort
	@echo finish
	@echo start lint
	-@$(MAKE) lint
	@echo start format
	-@$(MAKE) format

help: ## print this message
	@echo "Example operations by makefile."
	@echo ""
	@echo "Usage: make SUB_COMMAND argument_name=argument_value"
	@echo ""
	@echo "Command list:"
	@echo ""
	@printf "\033[36m%-30s\033[0m %-50s %s\n" "[Sub command]" "[Description]" "[Example]"
	@grep -E '^[/a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | perl -pe 's%^([/a-zA-Z_-]+):.*?(##)%$$1 $$2%' | awk -F " *?## *?" '{printf "\033[36m%-30s\033[0m %-50s %s\n", $$1, $$2, $$3}'
