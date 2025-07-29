FROM public.ecr.aws/lambda/python:3.11 
RUN yum update git -y
RUN yum install git -y
RUN yum groupinstall "Development Tools" -y

WORKDIR ${LAMBDA_TASK_ROOT}
COPY dashboard/queue_distribute/requirements.txt .
# A note on packages - the lambda ECR image is based on amazon linux, which only supports
# gcc 7.x - but numpy (and as a result, several numpy dependencies) have switched to
# only supporting gcc 8 or newer. So, until AWS backports gcc (and that might be a bit),
# pin dependencies in this requirement file to ensure builds will work.
RUN pip install -r requirements.txt
COPY dashboard/queue_distribute/queue_distribute.py .
COPY shared shared

# Force setup of some initial matplotlib configuration artifacts
RUN mkdir /tmp/matlplotlib
ENV MPLCONFIGDIR=/tmp/matlplotlib
RUN cumulus-library version

CMD ["queue_distribute.queue_handler"]