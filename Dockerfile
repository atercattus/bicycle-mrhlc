FROM alpine:3.5
ADD mr_hlc .
EXPOSE 80
CMD ./mr_hlc
