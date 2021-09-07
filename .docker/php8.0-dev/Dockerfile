ARG PHP_VER

FROM php:8.0-cli-alpine3.13

RUN apk update
RUN apk add --no-cache bash
RUN apk add --no-cache build-base
RUN apk add --no-cache autoconf
RUN apk add --no-cache automake

RUN apk add --no-cache --upgrade bash && \
    apk add libpq zip unzip tree

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer
RUN mkdir /.composer_cache
ENV COMPOSER_CACHE_DIR /.composer_cache

# curl for cmd
RUN apk add --no-cache curl

#develop layer
RUN pecl install xdebug
RUN docker-php-ext-enable xdebug

# bcmath
RUN docker-php-ext-install bcmath
RUN docker-php-ext-enable bcmath

# intl
RUN apk add --no-cache icu-dev
RUN docker-php-ext-install intl
RUN docker-php-ext-enable intl
