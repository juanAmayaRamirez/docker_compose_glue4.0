makedir:
	mkdir ~/glue_jupyter_workspace
	sudo chown -R 10000:10000 ~/glue_jupyter_workspace
run:
	docker-compose up -d
stop:
	docker-compose stop
