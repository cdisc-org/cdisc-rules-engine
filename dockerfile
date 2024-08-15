FROM python:3.10
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pyinstaller
EXPOSE 80
RUN echo '#!/bin/bash\n\
if [ -f /run/secrets/cdisc_api_key ]; then\n\
    export CDISC_LIBRARY_API_KEY=$(cat /run/secrets/cdisc_api_key)\n\
else\n\
    echo "Error: cdisc_api_key secret is not available"\n\
    exit 1\n\
fi\n\
python core.py update-cache\n\
python core.py validate -s "sdtmig" -v "3-3" -ct "sdtmct-2020-03-27" -d "/app/tests/test_suite" $(cat /app/tests/testrulelist.txt | sed "s/^/-r /")\n'\
> /app/run_all.sh
RUN chmod +x /app/run_all.sh
CMD ["/app/run_all.sh"]