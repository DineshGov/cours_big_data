FROM ubuntu:18.04
RUN apt update
RUN apt -y install sudo
RUN sudo apt -y install python3 python3-pip python3-venv git
RUN sudo apt -y install build-essential libssl-dev libffi-dev python3-dev

RUN git clone https://github.com/DineshGov/cours_big_data.git
WORKDIR /cours_big_data/

COPY requirements.txt .

RUN . bin/activate
RUN pip3 install -r requirements.txt

CMD ["sh", "-c" , "jupyter notebook --no-browser --port=8000 --ip='0.0.0.0' --allow-root"]
EXPOSE 8000
