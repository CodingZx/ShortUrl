create sequence url_idx_seq increment by 1 minvalue 1 no maxvalue start with 1;

CREATE TABLE "public"."url_record" (
  "id" int4 NOT NULL,
  "long_url" text COLLATE "pg_catalog"."default" NOT NULL,
  "create_time" timestamp(6) NOT NULL,
  CONSTRAINT "short_url_pkey" PRIMARY KEY ("id")
)
;

ALTER TABLE "public"."url_record"
  OWNER TO "postgres";
COMMENT ON COLUMN "public"."url_record"."long_url" IS '原始URL';
COMMENT ON COLUMN "public"."url_record"."create_time" IS '创建时间';