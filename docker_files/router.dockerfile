FROM alpine

RUN echo "net.ipv4.ip_forward=1" | tee -a /etc/sysctl.conf
# This next line will not work in a RUN command because sysctl needs to be executed while in runtime, not build time
# RUN sysctl -p

CMD [ "/bin/sh" ]
