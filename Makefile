.PHONY: install lint format test synth layers demo demo-down smoke

install:   ## install the locked dev environment (Python 3.12)
	uv sync

lint:      ## ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

format:    ## auto-format and apply safe lint fixes
	uv run ruff check --fix .
	uv run ruff format .

test:      ## unit + integration + CDK assertion tests (needs Node.js for the CDK tests)
	uv run pytest

synth:     ## cdk synth, offline (no AWS credentials needed)
	cd cdk && uv run --project .. -- npx --yes aws-cdk@2 synth --quiet

layers:    ## build the Lambda layers (required before `cdk deploy`)
	./scripts/build_layers.sh

demo:      ## whole system locally in Docker: http://localhost:$${WEB_PORT:-9090}
	docker compose -f local_app/docker-compose.yml up --build

demo-down:
	docker compose -f local_app/docker-compose.yml down --volumes

smoke:     ## start the demo, wait for trades, tear down (what CI runs)
	./scripts/smoke_local_demo.sh
