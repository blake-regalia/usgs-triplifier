# Dockerfile for USGS Triplifier
FROM node:12-stretch
MAINTAINER Blake Regalia <blake.regalia@gmail.com>

# source code
WORKDIR /src/app
COPY . .

# add PostgreSQL keys
RUN wget -q https://www.postgresql.org/media/keys/ACCC4CF8.asc -O - | apt-key add -
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" > /etc/apt/sources.list.d/pgdg.list

# install packages
RUN apt-get -y update \
    && apt-get upgrade -y \
    && apt-get install -yq \
        git \
        libpq-dev \
        postgresql-client-common \
        postgresql-client \
    && apt-get clean

# download GDAL 2
ENV GDAL_VERSION 2.4.1
RUN mkdir -p /src/gdal2
ADD http://download.osgeo.org/gdal/${GDAL_VERSION}/gdal-${GDAL_VERSION}.tar.gz /src/gdal2

# install GDAL 2
RUN cd /src/gdal2 \
    && tar -xvf gdal-${GDAL_VERSION}.tar.gz \
    && cd gdal-${GDAL_VERSION} \
    && ./configure --with-pg --with-curl \
    && make \
    && make install \
    && ldconfig \
    && rm -Rf /src/gdal2/*

# install software
RUN npm i

# entrypoint
ENTRYPOINT ["npm", "run"]
CMD ["all"]
