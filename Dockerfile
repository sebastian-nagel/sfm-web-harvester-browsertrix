#FROM gwul/sfm-base@sha256:e68cb98bdc9dc23bbed734f3e507a0ffb866b007dffea038b6af8d88a62150e6 as sfmbase
# upgraded to use python-3.9
# (build sfm-base image from https://github.com/sebastian-nagel/sfm-utils/tree/eo2)
FROM eo2/sfm-base:python-3.9 as sfmbase

# https://github.com/webrecorder/browsertrix-crawler
# built locally from:
#  https://github.com/sebastian-nagel/browsertrix-crawler/commits/eo2-collector
FROM eo2/browsertrix-crawler:latest

# fix browsertrix-crawler permissions (not running crawls as root)
RUN chmod a+rx /usr/bin/crawl \
    && chmod a+r /app/* \
	&& find /app/behaviors/ -type d -exec chmod a+rx {} \; \
	&& find /app/behaviors/ -type f -exec chmod a+r  {} \; \
	&& chmod a+rwx /crawls


### from sfm-utils/docker/base/Dockerfile
ARG DEBIAN_FRONTEND=noninteractive
# grab gosu for easy step-down from root
ENV GOSU_VERSION 1.11
RUN set -x \
	&& apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* \
	&& wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)" \
	&& wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& gpg --keyserver ha.pool.sks-keyservers.net --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 \
	&& gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu \
	&& command -v gpgconf && gpgconf --kill all || : \
	&& rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc \
	&& chmod +x /usr/local/bin/gosu \
	&& gosu nobody true
RUN pip install --upgrade ndg-httpsclient
RUN pip install appdeps==1.1.0
ENV SFM_REQS release
ENV DEBUG false
COPY --from=sfmbase /opt/sfm-setup/setup_reqs.sh /opt/sfm-setup/
######################################


ENV WORKDIR=/opt/sfm-web-harvester-browsertrix
COPY requirements $WORKDIR/requirements
RUN pip install \
		-r $WORKDIR/requirements/common.txt \
		-r $WORKDIR/requirements/release.txt

COPY docker/invoke.sh /opt/sfm-setup/
RUN chmod +x /opt/sfm-setup/invoke.sh

COPY *.py /opt/sfm-web-harvester-browsertrix/
WORKDIR /opt/sfm-web-harvester-browsertrix/

CMD ["/opt/sfm-setup/invoke.sh"]
