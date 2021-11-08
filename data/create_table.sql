-- DROP TABLE IF EXISTS public.stock_price;

CREATE TABLE IF NOT EXISTS public.stock_price
(
    id integer NOT NULL DEFAULT nextval('stock_price_id_seq'::regclass),
    name character(5)[] COLLATE pg_catalog."default" NOT NULL,
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
    foreign_sell integer NOT NULL,
    CONSTRAINT stock_price_pkey PRIMARY KEY (id)
)

