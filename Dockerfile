FROM ubuntu:24.04

# Base tools
RUN apt-get update && apt-get install -y \
    python3 python3-pip curl iproute2 wget unzip software-properties-common

# Add deadsnakes and install Python 3.13 (GIL) + 3.13-nogil
RUN add-apt-repository -y ppa:deadsnakes/ppa && apt-get update && apt-get install -y \
    python3.13 python3.13-venv python3.13-dev \
    python3.13-nogil libffi-dev

# Install pip for the nogil interpreter ONLY (don't touch apt's pip for 3.13)
RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
 && python3.13-nogil /tmp/get-pip.py

# Dependencies:
# - With GIL: use your full requirements (pandas, pyarrow, fastparquet)
# - With NO GIL: only pandas + pyarrow (skip fastparquet; it compiles and pulls git)
COPY requirements.txt /tmp/requirements.txt
RUN python3.13 -m pip install --no-cache-dir -r /tmp/requirements.txt
RUN python3.13 -m pip install --no-cache-dir pytest 
RUN python3.13-nogil -m pip install --no-cache-dir pandas pyarrow

# App code
WORKDIR /app
COPY app /app

# No default CMD; specify interpreter & args at docker run

