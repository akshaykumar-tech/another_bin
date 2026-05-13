--
-- PostgreSQL database dump
--

-- Dumped from database version 12.22 (Ubuntu 12.22-0ubuntu0.20.04.4)
-- Dumped by pg_dump version 12.22 (Ubuntu 12.22-0ubuntu0.20.04.4)

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

--
-- Data for Name: exchanges; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.exchanges (id, name, code, is_active, created_at, updated_at) FROM stdin;
1	Binance	binance	t	2026-04-14 02:38:03.493897	2026-04-14 02:38:03.493897
2	OKX	okx	t	2026-04-14 02:38:03.498609	2026-04-14 02:38:03.498609
4	Coinbase	coinbase	t	2026-04-20 06:36:49.744506	2026-04-20 06:36:49.744506
8	Upbit	upbit	t	2026-04-20 16:51:50.297994	2026-04-20 16:51:50.297994
\.


--
-- Data for Name: trading_settings; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.trading_settings (id, enabled, dry_run, leverage, allocation_percent, max_tokens_to_trade, first_partial_exit_after_minutes, second_partial_exit_after_minutes, first_exit_percent_of_qty, announcement_actions, created_at, updated_at, stop_loss_enabled, stop_loss_percent) FROM stdin;
1	t	f	10	25.00	10	5	10	50.00	{"hack": "open_short", "suspend": "none", "upgrade": "none", "delisting": "open_short", "token_burn": "open_short", "new_listing": "open_long", "market_support": "open_long", "monitoring_tag": "open_short", "attention_urged": "open_short", "investment_warning": "open_short"}	2026-04-18 03:37:05.465875	2026-05-01 00:57:57.401507	t	2.0000
\.


--
-- Name: exchanges_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.exchanges_id_seq', 8, true);


--
-- Name: trading_settings_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.trading_settings_id_seq', 1, true);


--
-- PostgreSQL database dump complete
--

