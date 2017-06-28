FROM perl:5.20
COPY . /usr/src/metadata-tools
WORKDIR /usr/src/metadata-tools
RUN curl -L http://cpanmin.us | perl - App::cpanminus
RUN cpanm Carton
RUN carton install
CMD [ "perl", "./hzn_export.pl" ]