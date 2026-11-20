# SemLake artifact: one image that rebuilds every number, table and figure of the paper
# and the PDF itself from the raw result files, or reruns the experiments.
#
#   docker build -t semlake .
#   docker run --rm -v "$PWD/out:/out" semlake            # paper from committed raw results
#   docker run --rm -v "$PWD/out:/out" semlake check      # + unit tests and decision check
#   docker run --rm -v "$PWD/out:/out" semlake quick      # smoke-run every experiment, small sizes
#   docker run --rm -v "$PWD/out:/out" semlake rerun      # rerun all local experiments (hours)
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MPLBACKEND=Agg \
    SEMLAKE_OUT=/out \
    PATH=/opt/venv/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv ca-certificates \
        texlive-latex-base texlive-latex-recommended texlive-latex-extra \
        texlive-fonts-recommended texlive-fonts-extra texlive-science \
        texlive-pictures texlive-bibtex-extra texlive-plain-generic cm-super poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/venv

WORKDIR /semlake
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock

COPY . .
RUN chmod +x reproduce.sh && mkdir -p /out

ENTRYPOINT ["./reproduce.sh"]
CMD ["paper"]
