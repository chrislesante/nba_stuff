FROM public.ecr.aws/lambda/python:3.10

ENV PYTHONPATH=/var/task/src:/var/task

WORKDIR ${LAMBDA_TASK_ROOT}

RUN yum update -y && \
    yum install -y postgresql-libs postgresql-devel gcc && \
    yum clean all

ENV LD_LIBRARY_PATH=/usr/lib:/usr/local/lib:$LD_LIBRARY_PATH

COPY requirements.txt ./
RUN python3 -m pip install -r requirements.txt

ARG LAMBDA_PACKAGE_DIR
COPY ${LAMBDA_PACKAGE_DIR} /var/task

COPY ./src ./src

RUN chmod +x ./src/scripts/refresh.sh

CMD ["src/scripts/lambda_function.lambda_handler"]