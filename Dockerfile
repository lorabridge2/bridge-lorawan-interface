FROM python:3.10-alpine as build

WORKDIR /home/lorawan
RUN apk add --no-cache gcc libc-dev g++
RUN pip install --no-cache-dir pipenv
COPY Pipfile* ./
RUN pipenv install --system --clear

FROM python:3.10-alpine
COPY --from=build /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY --from=build /usr/lib/libstdc++* /usr/lib/
WORKDIR /home/lorawan
COPY . .

USER 1337:1337
ENTRYPOINT [ "python3", "loratx.py"]
