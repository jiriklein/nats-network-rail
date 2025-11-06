local-stack:
	DOCKER_BUILDKIT=1 docker compose -f docker/docker-compose.yaml up --build --force-recreate
.PHONY: local-stack
