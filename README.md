# Glue 4.0 Docker compose and hudi
if using wsl2 or linux
## Set-up
1. Authenticate to aws and paste credentials in `.aws/` path in the repo. **[WARNING] Never commit .aws dir**
2. Add all custom python libraries to `./extra_python_path`
3. run the container with `docker-compose up -d` or with the **makefile** `make run`.
4. Open `localhost:8888`
5. There is a `sample.ipynb` with some sample code to test.
Credits:
https://aws.amazon.com/es/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/


