CREATE DATABASE IF NOT EXISTS weather_pipeline;

CREATE USER IF NOT EXISTS 'weather_user'@'%' IDENTIFIED BY 'choose_a_password';
GRANT ALL PRIVILEGES ON weather_pipeline.* TO 'weather_user'@'%';

USE weather_pipeline;

CREATE TABLE IF NOT EXISTS weather_daily (
    city             VARCHAR(20)  NOT NULL,
    day              DATE         NOT NULL,
    temp_min_c       DECIMAL(4,1),
    temp_max_c       DECIMAL(4,1),
    temp_avg_c       DECIMAL(4,1),
    precip_total_mm  DECIMAL(6,1),
    hours_recorded   INT,
    temp_avg_7d_c    DECIMAL(4,1),
    is_rainy_day     BOOLEAN,
    PRIMARY KEY (city, day)
);

CREATE TABLE IF NOT EXISTS weather_hourly (
    city              VARCHAR(20)  NOT NULL,
    observed_at       DATETIME     NOT NULL,
    temperature_c     DECIMAL(4,1),
    precipitation_mm  DECIMAL(5,1),
    mode              VARCHAR(10),
    extracted_on      DATE,
    PRIMARY KEY (city, observed_at)
);