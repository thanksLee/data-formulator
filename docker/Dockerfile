FROM python:3.13-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y curl git ca-certificates dos2unix

RUN git clone https://github.com/thanksLee/data-formulator.git -b develop/data-formulator

WORKDIR /app/data-formulator

# 스크립트 파일의 실행 권한 설정 및 확인
RUN chmod 755 local_server.sh && \
    ls -l local_server.sh

# nvm, node 설치
ENV NVM_DIR=/root/.nvm
ENV NODE_VERSION=22.14.0
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash && \
    . "$NVM_DIR/nvm.sh" && \
    nvm install $NODE_VERSION && \
    nvm alias default $NODE_VERSION && \
    nvm use default && \
    ln -s "$NVM_DIR/versions/node/v$NODE_VERSION/bin/node" /usr/local/bin/node && \
    ln -s "$NVM_DIR/versions/node/v$NODE_VERSION/bin/npm" /usr/local/bin/npm && \
    ln -s "$NVM_DIR/versions/node/v$NODE_VERSION/bin/npx" /usr/local/bin/npx

# yarn 설치 및 PATH 설정
RUN npm -g install yarn && \
    ln -s "$NVM_DIR/versions/node/v$NODE_VERSION/bin/yarn" /usr/local/bin/yarn && \
    ln -s "$NVM_DIR/versions/node/v$NODE_VERSION/bin/yarnpkg" /usr/local/bin/yarnpkg

# 프론트엔드 의존성 설치
RUN yarn install

# Python 가상환경 설정 및 백엔드 의존성 설치
RUN python -m venv .venv && \
    . .venv/bin/activate && \
    pip install -r requirements.txt

ENV VIRTUAL_ENV=/app/data-formulator/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONPATH=/app/data-formulator

# 스크립트 파일의 줄 끝 문자를 Unix 형식으로 변환하고 실행 권한 부여
RUN dos2unix local_server.sh && \
    chmod 755 local_server.sh && \
    echo '#!/bin/bash' | cat - local_server.sh > temp && \
    mv temp local_server.sh && \
    chmod 755 local_server.sh

# 시작 스크립트 생성
RUN echo '#!/bin/bash\n\
# 백엔드 서버 시작\n\
./local_server.sh &\n\
# 프론트엔드 서버 시작\n\
yarn start --host 0.0.0.0 --port 8000\n\
' > start.sh && \
    chmod +x start.sh

CMD ["./start.sh"]
