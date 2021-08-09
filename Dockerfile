FROM debian:bullseye-slim

RUN apt -y install python3-pip
# RUN python3 -m pip install -U pip
COPY / /plasmaflight
RUN cd /plasmaflight && pip3 install .

# FROM debian:bullseye
# RUN apt update && apt-get install -y python3 python3-six
# COPY --from=buildenv /usr/local /usr/local