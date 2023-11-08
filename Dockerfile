FROM ubuntu:22.04
#FROM ubuntu:18.04
CMD bash

# Install Ubuntu packages.
# Please add packages in alphabetical order.
ARG DEBIAN_FRONTEND=noninteractive
RUN  sed -i s@/archive.ubuntu.com/@/mirrors.aliyun.com/@g /etc/apt/sources.list
RUN  apt-get clean
RUN apt-get -y update && \
    apt-get -y install \
      build-essential \
      fuse libfuse-dev \
      sudo \
      clang-14 \
      clang-format-14 \
      clang-tidy-14 \
      cmake \
      doxygen \
      git gdb\
      g++-12 \
      pkg-config \
      zlib1g-dev && \
    update-alternatives --install /usr/bin/cc cc /usr/bin/clang-14 100 && \
    update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-14 100 && \
    ln -s /usr/bin/clang-format-14 /usr/bin/clang-format

COPY mypasswd /tmp

RUN useradd --no-log-init -r -m -g sudo stu

RUN cat /tmp/mypasswd | chpasswd

USER stu

WORKDIR /home/stu/