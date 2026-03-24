# Paths for simplified maintenance
DEV_COMPOSE := docker/dev/docker-compose.yml
DEV_ENV := docker/dev/.env.dev

# Production paths (project-relative)
PROD_COMPOSE := $(CURDIR)/docker/prod/docker-compose.yml
PROD_ENV := $(CURDIR)/docker/prod/.env.prod

# Use the modern Docker Compose plugin syntax
DOCKER_CMD := docker compose

# --- DEVELOPMENT COMMANDS ---

dev-build:
	$(DOCKER_CMD) -f $(DEV_COMPOSE) --env-file $(DEV_ENV) build

dev-up:
	$(DOCKER_CMD) -f $(DEV_COMPOSE) --env-file $(DEV_ENV) up -d

dev-rebuild:
	$(DOCKER_CMD) -f $(DEV_COMPOSE) --env-file $(DEV_ENV) up -d --build

dev-down:
	$(DOCKER_CMD) -f $(DEV_COMPOSE) down

dev-logs:
	$(DOCKER_CMD) -f $(DEV_COMPOSE) logs -f

# --- MAINTENANCE ---

clean:
	docker system prune -f
	docker volume prune -f

# --- PRODUCTION COMMANDS ---

prod-build:
	$(DOCKER_CMD) -f $(PROD_COMPOSE) --env-file $(PROD_ENV) build

prod-up:
	$(DOCKER_CMD) -f $(PROD_COMPOSE) --env-file $(PROD_ENV) up -d

prod-rebuild:
	$(DOCKER_CMD) -f $(PROD_COMPOSE) --env-file $(PROD_ENV) up -d --build

prod-down:
	$(DOCKER_CMD) -f $(PROD_COMPOSE) down

prod-logs:
	$(DOCKER_CMD) -f $(PROD_COMPOSE) logs -f
