# Use the latest Python 3 docker image
FROM python:3 as base

MAINTAINER Flywheel <support@flywheel.io>

RUN pip install flywheel-sdk==10.3.0 \
        pandas && \
    rm -rf /root/.cache/pip

# Make directory for flywheel spec (v0)
ENV FLYWHEEL /flywheel/v0
WORKDIR ${FLYWHEEL}

# Save docker environ
ENV PYTHONUNBUFFERED 1

# Copy executable/manifest to Gear
COPY manifest.json ${FLYWHEEL}/manifest.json
COPY run.py ${FLYWHEEL}/run.py

# Configure entrypoint
RUN chmod a+x ${FLYWHEEL}/run.py
ENTRYPOINT ['/flywheel/v0/run.py']
#RUN sed -i 's/ThreadPool/Pool/g' /usr/local/lib/python3.8/site-packages/flywheel/api_client.py
