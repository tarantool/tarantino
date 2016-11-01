FROM tarantool/tarantool:1.7

RUN mkdir /opt/tarantool/tarantino
COPY src/*.lua /opt/tarantool/tarantino
COPY dist/docker.lua /opt/tarantool
COPY dist/service.json /opt/tarantool

EXPOSE 5301

CMD ["/usr/local/bin/tarantool", "/opt/tarantool/docker.lua"]
