PYTHON=python3
PYTEST=pytest
MAIN_SCRIPT=autoperf.py

all: test

test:
	$(PYTEST) --cov=PTST . 

run: 
	$(PYTHON) $(MAIN_SCRIPT)

clean:
	find . -type f -name '*.pyc' -delete
	find . -type f -name '__pycache__' -delete
	rm -rf .pytest_cache
	rm -rf htmlcov

.PHONY: all test clean
