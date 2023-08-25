# Glue 4.0 Docker compose and hudi
if using wsl2 or linux
## Set-up
1. Create a directory that jupyter will use to save the notebooks and scripts.  
Make sure to change the owner of the directory with sudo chown -R 10000:10000 ~/glue_jupyter_workspace
this can be made with **makefile** `make makedir`
2. Edit **line 8** of the **docker.compose.yml** file to point to the corresponding profile stored in `~/.aws/credentials`
3. Add all custom python libraries to `./extra_python_path`
4. run the container with `docker-compose up -d` or with **makefile** `make run`.
5. Open `localhost:8080`


https://aws.amazon.com/es/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/


