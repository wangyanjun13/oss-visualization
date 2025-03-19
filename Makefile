SRC_DIR=.

.PHONY: build test dockerfile-prepare

build:
	docker build -t $(IMAGE_URL):$(DOCKER_TAG) --build-arg COMMIT_ID=$(COMMIT_ID) --cache-from $(IMAGE_URL):latest .

test:
	echo "Testing for project $(DOCKER_TAG)"
	python -m pytest tests/

clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name "*.egg" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".coverage" -exec rm -r {} +

run:
	uvicorn app:app --host 0.0.0.0 --port 5001 --workers 4 --timeout-keep-alive 75

run-dev:
	uvicorn app:app --host 0.0.0.0 --port 5001 --reload --timeout-keep-alive 75

dockerfile-prepare:
	@echo "Preparing for docker build..."
	@if [ ! -f requirements.txt ]; then \
		echo "Error: requirements.txt not found"; \
		exit 1; \
	fi
	@if [ ! -d rclone ]; then \
		echo "Error: rclone directory not found"; \
		exit 1; \
	fi
	@echo "All prerequisites checked, ready for docker build"