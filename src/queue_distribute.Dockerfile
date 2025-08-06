FROM public.ecr.aws/lambda/python:3.11 
RUN yum update git -y
RUN yum install git -y
RUN yum groupinstall "Development Tools" -y

WORKDIR ${LAMBDA_TASK_ROOT}
COPY dashboard/queue_distribute/requirements.txt .
RUN pip install -r requirements.txt
COPY dashboard/queue_distribute/queue_distribute.py .
COPY shared shared

# Force setup of some initial matplotlib configuration artifacts
RUN mkdir /tmp/matlplotlib
ENV MPLCONFIGDIR=/tmp/matlplotlib
RUN cumulus-library version

CMD ["queue_distribute.queue_handler"]