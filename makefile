PROJECT_NAME = AutoPerf
PYTHON = python3
CONFIG_PATH = config/vms/devtest.toml

all: run

run:
	@echo "Running AutoPerf..."
	$(PYTHON) run.py $(CONFIG_PATH)

test:
	@echo "Running tests..."
	$(PYTHON) -m unittest discover -s tests

clean:
	@echo "Cleaning up..."
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.pyc" -type f -delete
	find . -name "*.pyo" -type f -delete

help:
	@echo "Usage: make [target]"
	@echo "Targets:"
	@echo "  run: Run AutoPerf"
	@echo "  test: Run tests"
	@echo "  clean: Clean up"
	@echo "  help: Show this help message"
