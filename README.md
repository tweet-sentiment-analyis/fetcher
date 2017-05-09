# Tweet Fetcher

This component fetches tweets from twitter. 

## Build and Deploy
In order to build this project as well as deploy a new image to docker hub, perform the following steps:

* Invoke `mvn package` to install and package the project into a JAR located in the target folder.

* Login to your account to dockerhub: `docker login`
* From the root of this project, build a new docker image with the respective version: `docker build -t tweetsentimentanalysis/fetcher:0.0.1 .`
* Run `docker push tweetsentimentanalysis/fetcher:0.0.1` to deploy to dockerhub

## Run the container
After you have built the container, you may run it using the following command: `docker run -p 8090:8080 -e AWS_ACCESS_KEY_ID='...' -e AWS_SECRET_ACCESS_KEY='...' tweetsentimentanalysis/fetcher:0.0.1` which runs the container and lets you connect to it [http://192.168.99.100:8090](http://192.168.99.100:8090).

## Remove built images
In order to clean up, you may want to remove the previously created image and container:

* Run `docker ps -a` in order list created containers.
* Choose the corresponding container id and invoke `docker stop <ID> && docker rm <ID>`
* Run `docker images` to list all created images
* Run `docker rmi <ID>` with the id of the image to remove it
