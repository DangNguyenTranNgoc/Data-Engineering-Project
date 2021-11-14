--
-- PostgreSQL database dump
--

-- Dumped from database version 13.5 (Ubuntu 13.5-1.pgdg20.04+1)
-- Dumped by pg_dump version 14.1 (Ubuntu 14.1-1.pgdg20.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

CREATE TABLE public.stock_info (
    id integer NOT NULL,
    code character(5) NOT NULL,
    company_name character(255) NOT NULL,
    pub_date date NOT NULL,
    first_vol real NOT NULL,
    pub_price real NOT NULL,
    curr_vol real NOT NULL,
    treasury_shares real NOT NULL,
    pub_vol real NOT NULL,
    fr_owner real NOT NULL,
    fr_owner_rat real NOT NULL,
    fr_vol_remain real NOT NULL,
    curr_price real NOT NULL,
    market_capital real NOT NULL
);

ALTER TABLE public.stock_info OWNER TO postgres;

CREATE SEQUENCE public.stock_info_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE public.stock_info_id_seq OWNER TO postgres;

ALTER SEQUENCE public.stock_info_id_seq OWNED BY public.stock_info.id;


CREATE TABLE public.stock_price (
    id integer NOT NULL,
    code character(5) NOT NULL,
    date date NOT NULL,
    ref_price real NOT NULL,
    diff_price real NOT NULL,
    diff_price_rat real NOT NULL,
    close_price real NOT NULL,
    vol integer NOT NULL,
    open_price real NOT NULL,
    highest_price real NOT NULL,
    lowest_price real NOT NULL,
    transaction integer NOT NULL,
    foreign_buy integer NOT NULL,
    foreign_sell integer NOT NULL
);


ALTER TABLE public.stock_price OWNER TO postgres;

CREATE SEQUENCE public.stock_price_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.stock_price_id_seq OWNER TO postgres;

ALTER SEQUENCE public.stock_price_id_seq OWNED BY public.stock_price.id;

ALTER TABLE ONLY public.stock_info ALTER COLUMN id SET DEFAULT nextval('public.stock_info_id_seq'::regclass);

ALTER TABLE ONLY public.stock_price ALTER COLUMN id SET DEFAULT nextval('public.stock_price_id_seq'::regclass);

SELECT pg_catalog.setval('public.stock_info_id_seq', 1, false);

SELECT pg_catalog.setval('public.stock_price_id_seq', 1, false);

ALTER TABLE ONLY public.stock_info
    ADD CONSTRAINT stock_info_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.stock_price
    ADD CONSTRAINT stock_price_pkey PRIMARY KEY (id);
