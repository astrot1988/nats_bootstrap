FROM ruby:3.4.5-slim-bookworm AS base

RUN apt update && apt --fix-missing install -y build-essential bash curl && rm -rf /var/lib/apt/lists/*

RUN echo 'gem: --no-document' >> ~/.gemrc

WORKDIR /app

COPY Gemfile* /app/

RUN bundle install --jobs $(nproc) --retry=3 && \
    bundle clean --force && rm -rf /usr/local/bundle/cache/*

COPY . /app

CMD ls && bundle exec ruby main.rb
