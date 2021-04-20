# Browsertrix Web Harvester for Social Feed Manager

Harvester for web content as part of [Social Feed Manager](https://gwu-libraries.github.io/sfm-ui/) based on the [Browsertrix Crawler](https://github.com/webrecorder/browsertrix-crawler).



## Build Development Version

To build the development version (see also the [Dockerfile](./Dockerfile):
```
cd ..
test -d sfm-utils \
  || git clone https://github.com/sebastian-nagel/sfm-utils.git
cd sfm-utils
git checkout eo2-collector
cd docker/base
docker build -t eo2/sfm-base:latest .
cd ../../..

test -d browsertrix-crawler \
  || git clone https://github.com/sebastian-nagel/browsertrix-crawler
cd browsertrix-crawler
git checkout eo2-collector
docker build -t eo2/browsertrix-crawler:latest .
cd ..

cd sfm-web-harvester-browsertrix   # this repository
docker build -t eo2/browsertrixharvester:latest .
```

