FROM centos:7

WORKDIR /usr/src/app 
 
RUN yum -y -q update \
    && yum -y -q install epel-release \
    && yum -y -q install \
        rpm-python \
        python2-configargparse \
        python-requests \
        python-requests-kerberos \
        python-urllib3 \
        pyOpenSSL \
        python-six \
        python2-pip \
    && yum -y -q clean all \
    && rm -rf /var/cache/yum \
    && pip install python-gitlab

VOLUME ["/usr/src/app/certs"]

COPY koji_runner.py .
COPY koji .
 
CMD [ "python", "./koji_runner.py" ]
