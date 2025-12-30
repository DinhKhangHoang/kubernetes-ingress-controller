FROM amd64/golang:1.24.3 as builder
WORKDIR /workspaces
RUN ln -f -s /usr/share/zoneinfo/Asia/Ho_Chi_Minh /etc/localtime &&\
    apt update &&\
    apt install -y \
        jq \    
        zip \    
        unzip \    
        vim \
        git \
        curl
EXPOSE 8080