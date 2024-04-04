FROM python:3.12
ADD . /src
WORKDIR /src

COPY requirements.txt ./
RUN pip install --upgrade pip && \
	pip install -r requirements.txt

COPY . .
CMD [ "python", "./main.py"]