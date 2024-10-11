FROM python:3.10-slim

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/app
RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY requirements.txt /opt/dagster/app/

RUN pip install -r /opt/dagster/app/requirements.txt
ENV DAGSTER_HOME=/opt/dagster/dagster_home/
ENV PYTHONPATH="/opt/dagster/app:${PYTHONPATH}"

COPY newyork_cab_analytics workspace.yaml /opt/dagster/app/

COPY dagster.yaml  /opt/dagster/dagster_home/

WORKDIR /opt/dagster/app
EXPOSE 3000

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]