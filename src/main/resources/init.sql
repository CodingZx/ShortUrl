create sequence url_idx_seq increment by 1 minvalue 1 no maxvalue start with 1;

CREATE TABLE "public"."short_url" (
  "id" int4 NOT NULL,
  "long_url" text COLLATE "pg_catalog"."default" NOT NULL,
  "create_time" timestamp(6) NOT NULL,
  "short_url" varchar(50) COLLATE "pg_catalog"."default" NOT NULL DEFAULT ''::character varying,
  CONSTRAINT "short_url_pkey" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."short_url"
  OWNER TO "postgres";

COMMENT ON COLUMN "public"."short_url"."long_url" IS '原始URL';

COMMENT ON COLUMN "public"."short_url"."create_time" IS '创建时间';

COMMENT ON COLUMN "public"."short_url"."short_url" IS '短链接';