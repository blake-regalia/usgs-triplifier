# Dockerfile for USGS Triplifier
FROM node:9
MAINTAINER Blake Regalia <blake.regalia@gmail.com>

# prepare the environment
RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
    	git

# USGS Triplifier source code
WORKDIR /src
RUN git clone https://github.com/blake-regalia/usgs-triplifier.git /src

# install
RUN npm i

# entrypoint
ENTRYPOINT ["npm", "run"]
CMD ["all"]