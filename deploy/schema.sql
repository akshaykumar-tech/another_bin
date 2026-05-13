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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: active_admin_comments; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.active_admin_comments (
    id bigint NOT NULL,
    namespace character varying,
    body text,
    resource_type character varying,
    resource_id bigint,
    author_type character varying,
    author_id bigint,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.active_admin_comments OWNER TO postgres;

--
-- Name: active_admin_comments_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.active_admin_comments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.active_admin_comments_id_seq OWNER TO postgres;

--
-- Name: active_admin_comments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.active_admin_comments_id_seq OWNED BY public.active_admin_comments.id;


--
-- Name: admin_users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.admin_users (
    id bigint NOT NULL,
    email character varying DEFAULT ''::character varying NOT NULL,
    encrypted_password character varying DEFAULT ''::character varying NOT NULL,
    reset_password_token character varying,
    reset_password_sent_at timestamp(6) without time zone,
    remember_created_at timestamp(6) without time zone,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.admin_users OWNER TO postgres;

--
-- Name: admin_users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.admin_users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.admin_users_id_seq OWNER TO postgres;

--
-- Name: admin_users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.admin_users_id_seq OWNED BY public.admin_users.id;


--
-- Name: alert_configurations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.alert_configurations (
    id bigint NOT NULL,
    phone_number character varying NOT NULL,
    is_active boolean DEFAULT true,
    enabled_exchanges jsonb DEFAULT '{"okx": true, "binance": true, "coinbase": true}'::jsonb,
    enabled_announcement_types jsonb DEFAULT '{"hack": true, "suspend": true, "upgrade": false, "delisting": true, "token_burn": true, "new_listing": true}'::jsonb,
    min_priority integer,
    last_alert_at timestamp(6) without time zone,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.alert_configurations OWNER TO postgres;

--
-- Name: alert_configurations_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.alert_configurations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.alert_configurations_id_seq OWNER TO postgres;

--
-- Name: alert_configurations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.alert_configurations_id_seq OWNED BY public.alert_configurations.id;


--
-- Name: announcements; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.announcements (
    id bigint NOT NULL,
    exchange_id bigint NOT NULL,
    title character varying NOT NULL,
    content text,
    announcement_type character varying,
    severity character varying,
    published_at timestamp(6) without time zone,
    affected_tokens character varying[] DEFAULT '{}'::character varying[],
    effective_date timestamp(6) without time zone,
    alert_sent boolean DEFAULT false,
    raw_data jsonb,
    price_impact_percentage integer,
    recommended_action character varying,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.announcements OWNER TO postgres;

--
-- Name: announcements_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.announcements_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.announcements_id_seq OWNER TO postgres;

--
-- Name: announcements_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.announcements_id_seq OWNED BY public.announcements.id;


--
-- Name: ar_internal_metadata; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ar_internal_metadata (
    key character varying NOT NULL,
    value character varying,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.ar_internal_metadata OWNER TO postgres;

--
-- Name: exchanges; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.exchanges (
    id bigint NOT NULL,
    name character varying,
    code character varying,
    is_active boolean DEFAULT true,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.exchanges OWNER TO postgres;

--
-- Name: exchanges_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.exchanges_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.exchanges_id_seq OWNER TO postgres;

--
-- Name: exchanges_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.exchanges_id_seq OWNED BY public.exchanges.id;


--
-- Name: hf_flow_micro; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hf_flow_micro (
    id bigint NOT NULL,
    symbol text NOT NULL,
    score integer NOT NULL,
    direction_pressure text NOT NULL,
    spread_bps double precision,
    touch_imbalance double precision,
    depth_imbalance double precision,
    quote_vol_24h double precision,
    chg_24h_pct double precision,
    bid_notional_touch double precision,
    ask_notional_touch double precision,
    bid_notional_band double precision,
    ask_notional_band double precision,
    reason text,
    meta jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.hf_flow_micro OWNER TO postgres;

--
-- Name: hf_flow_micro_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hf_flow_micro_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.hf_flow_micro_id_seq OWNER TO postgres;

--
-- Name: hf_flow_micro_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hf_flow_micro_id_seq OWNED BY public.hf_flow_micro.id;


--
-- Name: hf_spike_prob; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hf_spike_prob (
    id bigint NOT NULL,
    symbol text NOT NULL,
    prob integer NOT NULL,
    direction_hint text NOT NULL,
    vol_ratio double precision,
    short_vol double precision,
    baseline_vol double precision,
    short_range_pct double precision,
    funding_rate double precision,
    horizon_sec integer DEFAULT 300 NOT NULL,
    reason text,
    meta jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.hf_spike_prob OWNER TO postgres;

--
-- Name: hf_spike_prob_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hf_spike_prob_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.hf_spike_prob_id_seq OWNER TO postgres;

--
-- Name: hf_spike_prob_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hf_spike_prob_id_seq OWNED BY public.hf_spike_prob.id;


--
-- Name: hf_trade_signals; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.hf_trade_signals (
    id bigint NOT NULL,
    symbol text NOT NULL,
    direction text NOT NULL,
    signal_class text NOT NULL,
    confidence integer DEFAULT 0 NOT NULL,
    range_pct double precision,
    move_pct_60s double precision,
    pump_score integer DEFAULT 0 NOT NULL,
    dump_score integer DEFAULT 0 NOT NULL,
    funding_rate double precision,
    spread_bps double precision,
    reason text,
    meta jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.hf_trade_signals OWNER TO postgres;

--
-- Name: hf_trade_signals_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.hf_trade_signals_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.hf_trade_signals_id_seq OWNER TO postgres;

--
-- Name: hf_trade_signals_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.hf_trade_signals_id_seq OWNED BY public.hf_trade_signals.id;


--
-- Name: paper_trade_logs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.paper_trade_logs (
    id bigint NOT NULL,
    symbol text NOT NULL,
    side text NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    entry_price double precision,
    exit_price double precision,
    margin_usdt double precision,
    leverage double precision,
    notional_usdt double precision,
    net_pnl_usdt double precision,
    net_pct_notional double precision,
    exit_reason text,
    metrics jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    closed_at timestamp with time zone
);


ALTER TABLE public.paper_trade_logs OWNER TO postgres;

--
-- Name: paper_trade_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.paper_trade_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.paper_trade_logs_id_seq OWNER TO postgres;

--
-- Name: paper_trade_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.paper_trade_logs_id_seq OWNED BY public.paper_trade_logs.id;


--
-- Name: price_movements; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.price_movements (
    id bigint NOT NULL,
    announcement_id bigint NOT NULL,
    token_symbol character varying,
    price_before numeric,
    price_after_1h numeric,
    price_after_24h numeric,
    percentage_change_1h numeric,
    percentage_change_24h numeric,
    measured_at timestamp(6) without time zone,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.price_movements OWNER TO postgres;

--
-- Name: price_movements_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.price_movements_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.price_movements_id_seq OWNER TO postgres;

--
-- Name: price_movements_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.price_movements_id_seq OWNED BY public.price_movements.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


ALTER TABLE public.schema_migrations OWNER TO postgres;

--
-- Name: trade_executions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.trade_executions (
    id bigint NOT NULL,
    announcement_id bigint NOT NULL,
    symbol character varying NOT NULL,
    base_asset character varying NOT NULL,
    position_side character varying DEFAULT 'short'::character varying NOT NULL,
    status character varying DEFAULT 'pending'::character varying NOT NULL,
    quantity numeric(28,8),
    quantity_remaining numeric(28,8),
    entry_price numeric(28,8),
    binance_client_order_id character varying,
    last_error text,
    manual_close_requested boolean DEFAULT false NOT NULL,
    raw_response jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL
);


ALTER TABLE public.trade_executions OWNER TO postgres;

--
-- Name: trade_executions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.trade_executions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.trade_executions_id_seq OWNER TO postgres;

--
-- Name: trade_executions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.trade_executions_id_seq OWNED BY public.trade_executions.id;


--
-- Name: trading_settings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.trading_settings (
    id bigint NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    dry_run boolean DEFAULT true NOT NULL,
    leverage integer DEFAULT 5 NOT NULL,
    allocation_percent numeric(8,2) DEFAULT 65.0 NOT NULL,
    max_tokens_to_trade integer DEFAULT 5 NOT NULL,
    first_partial_exit_after_minutes integer DEFAULT 5 NOT NULL,
    second_partial_exit_after_minutes integer DEFAULT 10 NOT NULL,
    first_exit_percent_of_qty numeric(8,2) DEFAULT 50.0 NOT NULL,
    announcement_actions jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    stop_loss_enabled boolean DEFAULT true NOT NULL,
    stop_loss_percent numeric(8,4) DEFAULT 2.0 NOT NULL
);


ALTER TABLE public.trading_settings OWNER TO postgres;

--
-- Name: trading_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.trading_settings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.trading_settings_id_seq OWNER TO postgres;

--
-- Name: trading_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.trading_settings_id_seq OWNED BY public.trading_settings.id;


--
-- Name: active_admin_comments id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.active_admin_comments ALTER COLUMN id SET DEFAULT nextval('public.active_admin_comments_id_seq'::regclass);


--
-- Name: admin_users id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.admin_users ALTER COLUMN id SET DEFAULT nextval('public.admin_users_id_seq'::regclass);


--
-- Name: alert_configurations id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alert_configurations ALTER COLUMN id SET DEFAULT nextval('public.alert_configurations_id_seq'::regclass);


--
-- Name: announcements id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.announcements ALTER COLUMN id SET DEFAULT nextval('public.announcements_id_seq'::regclass);


--
-- Name: exchanges id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.exchanges ALTER COLUMN id SET DEFAULT nextval('public.exchanges_id_seq'::regclass);


--
-- Name: hf_flow_micro id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hf_flow_micro ALTER COLUMN id SET DEFAULT nextval('public.hf_flow_micro_id_seq'::regclass);


--
-- Name: hf_spike_prob id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hf_spike_prob ALTER COLUMN id SET DEFAULT nextval('public.hf_spike_prob_id_seq'::regclass);


--
-- Name: hf_trade_signals id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hf_trade_signals ALTER COLUMN id SET DEFAULT nextval('public.hf_trade_signals_id_seq'::regclass);


--
-- Name: paper_trade_logs id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.paper_trade_logs ALTER COLUMN id SET DEFAULT nextval('public.paper_trade_logs_id_seq'::regclass);


--
-- Name: price_movements id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.price_movements ALTER COLUMN id SET DEFAULT nextval('public.price_movements_id_seq'::regclass);


--
-- Name: trade_executions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trade_executions ALTER COLUMN id SET DEFAULT nextval('public.trade_executions_id_seq'::regclass);


--
-- Name: trading_settings id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trading_settings ALTER COLUMN id SET DEFAULT nextval('public.trading_settings_id_seq'::regclass);


--
-- Name: active_admin_comments active_admin_comments_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.active_admin_comments
    ADD CONSTRAINT active_admin_comments_pkey PRIMARY KEY (id);


--
-- Name: admin_users admin_users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.admin_users
    ADD CONSTRAINT admin_users_pkey PRIMARY KEY (id);


--
-- Name: alert_configurations alert_configurations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.alert_configurations
    ADD CONSTRAINT alert_configurations_pkey PRIMARY KEY (id);


--
-- Name: announcements announcements_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.announcements
    ADD CONSTRAINT announcements_pkey PRIMARY KEY (id);


--
-- Name: ar_internal_metadata ar_internal_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ar_internal_metadata
    ADD CONSTRAINT ar_internal_metadata_pkey PRIMARY KEY (key);


--
-- Name: exchanges exchanges_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.exchanges
    ADD CONSTRAINT exchanges_pkey PRIMARY KEY (id);


--
-- Name: hf_flow_micro hf_flow_micro_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hf_flow_micro
    ADD CONSTRAINT hf_flow_micro_pkey PRIMARY KEY (id);


--
-- Name: hf_spike_prob hf_spike_prob_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hf_spike_prob
    ADD CONSTRAINT hf_spike_prob_pkey PRIMARY KEY (id);


--
-- Name: hf_trade_signals hf_trade_signals_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.hf_trade_signals
    ADD CONSTRAINT hf_trade_signals_pkey PRIMARY KEY (id);


--
-- Name: paper_trade_logs paper_trade_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.paper_trade_logs
    ADD CONSTRAINT paper_trade_logs_pkey PRIMARY KEY (id);


--
-- Name: price_movements price_movements_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.price_movements
    ADD CONSTRAINT price_movements_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: trade_executions trade_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trade_executions
    ADD CONSTRAINT trade_executions_pkey PRIMARY KEY (id);


--
-- Name: trading_settings trading_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trading_settings
    ADD CONSTRAINT trading_settings_pkey PRIMARY KEY (id);


--
-- Name: idx_hf_flow_micro_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_flow_micro_created ON public.hf_flow_micro USING btree (created_at DESC);


--
-- Name: idx_hf_flow_micro_score; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_flow_micro_score ON public.hf_flow_micro USING btree (score DESC);


--
-- Name: idx_hf_flow_micro_symbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_flow_micro_symbol ON public.hf_flow_micro USING btree (symbol);


--
-- Name: idx_hf_spike_prob_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_spike_prob_created ON public.hf_spike_prob USING btree (created_at DESC);


--
-- Name: idx_hf_spike_prob_prob; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_spike_prob_prob ON public.hf_spike_prob USING btree (prob DESC);


--
-- Name: idx_hf_spike_prob_symbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_spike_prob_symbol ON public.hf_spike_prob USING btree (symbol);


--
-- Name: idx_hf_trade_signals_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_trade_signals_created ON public.hf_trade_signals USING btree (created_at DESC);


--
-- Name: idx_hf_trade_signals_symbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_hf_trade_signals_symbol ON public.hf_trade_signals USING btree (symbol);


--
-- Name: idx_paper_trade_logs_created; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paper_trade_logs_created ON public.paper_trade_logs USING btree (created_at DESC);


--
-- Name: idx_paper_trade_logs_status; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paper_trade_logs_status ON public.paper_trade_logs USING btree (status);


--
-- Name: idx_paper_trade_logs_symbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_paper_trade_logs_symbol ON public.paper_trade_logs USING btree (symbol);


--
-- Name: index_active_admin_comments_on_author; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_active_admin_comments_on_author ON public.active_admin_comments USING btree (author_type, author_id);


--
-- Name: index_active_admin_comments_on_namespace; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_active_admin_comments_on_namespace ON public.active_admin_comments USING btree (namespace);


--
-- Name: index_active_admin_comments_on_resource; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_active_admin_comments_on_resource ON public.active_admin_comments USING btree (resource_type, resource_id);


--
-- Name: index_admin_users_on_email; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX index_admin_users_on_email ON public.admin_users USING btree (email);


--
-- Name: index_admin_users_on_reset_password_token; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX index_admin_users_on_reset_password_token ON public.admin_users USING btree (reset_password_token);


--
-- Name: index_announcements_on_exchange_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_announcements_on_exchange_id ON public.announcements USING btree (exchange_id);


--
-- Name: index_price_movements_on_announcement_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_price_movements_on_announcement_id ON public.price_movements USING btree (announcement_id);


--
-- Name: index_trade_executions_on_announcement_and_symbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_trade_executions_on_announcement_and_symbol ON public.trade_executions USING btree (announcement_id, symbol);


--
-- Name: index_trade_executions_on_announcement_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX index_trade_executions_on_announcement_id ON public.trade_executions USING btree (announcement_id);


--
-- Name: price_movements fk_rails_16dc12c680; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.price_movements
    ADD CONSTRAINT fk_rails_16dc12c680 FOREIGN KEY (announcement_id) REFERENCES public.announcements(id);


--
-- Name: trade_executions fk_rails_8194c681d3; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.trade_executions
    ADD CONSTRAINT fk_rails_8194c681d3 FOREIGN KEY (announcement_id) REFERENCES public.announcements(id);


--
-- Name: announcements fk_rails_90619f67a4; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.announcements
    ADD CONSTRAINT fk_rails_90619f67a4 FOREIGN KEY (exchange_id) REFERENCES public.exchanges(id);


--
-- PostgreSQL database dump complete
--

