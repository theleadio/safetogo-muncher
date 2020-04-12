FROM python:3.7-stretch
COPY ./services /root/services
COPY ./.env /root/.env
COPY ./requirements.txt /root/
COPY ./outbreak/outbreak.py /root/
RUN python3 -m pip install -r /root/requirements.txt
CMD [ "python3" , "/root/outbreak.py"]