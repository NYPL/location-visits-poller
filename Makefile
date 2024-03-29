.DEFAULT: help

help:
	@echo "make help"
	@echo "    display this help statement"
	@echo "make run"
	@echo "    run the application in development mode"
	@echo "make test"
	@echo "    run associated test suite with pytest"
	@echo "make lint"
	@echo "    lint project files using the black linter"

run:
	export ENVIRONMENT=devel; \
	python main.py

test:
	pytest tests

lint:
	black ./ --check --exclude="(env/)|(tests/)"