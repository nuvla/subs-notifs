FROM python:3.8-alpine3.12

ARG GIT_BRANCH
ARG GIT_COMMIT_ID
ARG GIT_BUILD_TIME
ARG GITHUB_RUN_NUMBER
ARG GITHUB_RUN_ID
ARG PROJECT_URL

LABEL git.branch=${GIT_BRANCH}
LABEL git.commit.id=${GIT_COMMIT_ID}
LABEL git.build.time=${GIT_BUILD_TIME}
LABEL git.run.number=${GITHUB_RUN_NUMBER}
LABEL git.run.id=${GITHUB_RUN_ID}
LABEL org.opencontainers.image.authors="support@sixsq.com"
LABEL org.opencontainers.image.created=${GIT_BUILD_TIME}
LABEL org.opencontainers.image.url=${PROJECT_URL}
LABEL org.opencontainers.image.vendor="SixSq SA"
LABEL org.opencontainers.image.title="Subscriptions to telemetry notifications"
LABEL org.opencontainers.image.description="Based on a stream of telemetry and user subscriptions produces notifications."

COPY nuvla/notifs requirements.txt subs-notifs.py subs-notifs-monitoring.py LICENSE /opt/nuvla/

WORKDIR /opt/nuvla/

RUN pip install --no-cache-dir -r requirements.txt

RUN mv /opt/nuvla/notifs /usr/local/lib/python3.8/site-packages/nuvla/ \
    && rm -rf /opt/nuvla/notifs

VOLUME /srv/nuvla/shared
ENV PYTHONPATH=/opt/nuvla/

EXPOSE 9137
ENTRYPOINT ["./subs-notifs.py"]
