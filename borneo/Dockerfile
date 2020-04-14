FROM python:3.7-slim-buster
RUN apt-get update
RUN apt-get install default-libmysqlclient-dev libevent-dev gcc -y
COPY ./services /root/services
COPY ./.env /root/.env
COPY ./borneo/requirements.txt /root/
COPY ./borneo/borneo.py /root/
RUN python3 -m pip install -r /root/requirements.txt
CMD [ "python3" , "/root/borneo.py"]