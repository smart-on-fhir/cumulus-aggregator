FROM public.ecr.aws/lambda/python:3.11

WORKDIR ${LAMBDA_TASK_ROOT}
COPY dashboard/post_distribute/requirements.txt .
RUN pip install -r requirements.txt
COPY dashboard/post_distribute/post_distribute.py .
COPY shared shared

# Force setup of some initial matplotlib configuration artifacts
RUN mkdir /tmp/matlplotlib
ENV MPLCONFIGDIR=/tmp/matlplotlib
RUN cumulus-library version

CMD ["post_distribute.distribute_handler"]