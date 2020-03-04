SHELL := /bin/bash

build:
	docker-compose up -d
	go run ./breweries/main.go
	go build -o beerfan