# Glue 3.0 Docker compose
if using wsl2 or linux
## Set-up
1. Create a directory that jupyter will use to save the notebooks and scripts.  
Make sure to change the owner of the directory with sudo chown -R 10000:10000 ~/glue_jupyter_workspace
this can be made with **makefile** `make makedir`
2. Edit **line 8** of the **docker.compose.yml** file to point to the corresponding profile stored in `~/.aws/credentials`
3. run the container with `docker-compose up -d` or with **makefile** `make run`.
4. Open `localhost:8080`