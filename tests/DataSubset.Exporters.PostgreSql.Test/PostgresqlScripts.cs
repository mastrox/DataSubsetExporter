using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql.Test
{
    internal static class PostgresqlScripts
    {
        public static string createTable = @"CREATE TABLE {0}.typetest (
					id int  NOT NULL,
					bool_nn bool NOT NULL,
					bool_n bool NULL,
					boolean_nn boolean NOT NULL,
					boolean_n boolean NULL,
					bigint_nn bigint NOT NULL,
					bigint_n bigint NULL,
					bigserial_nn bigserial NOT NULL,
					bit_nn bit NOT NULL,
					bit_n bit NULL,
					bit_varying_nn bit varying NOT NULL,
					bit_varing_n bit varying NULL,
					box_nn box NOT NULL,
					box_n box NULL,
					char_nn char NOT NULL,
					char_n char NULL,
					char_varying_nn char varying NOT NULL,
					char_varying_n char varying NULL,
					character_nn character NOT NULL,
					character_n character NULL,
					character_varying_nn character varying NOT NULL,
					character_varying_n character varying NULL,
					date_nn date NOT NULL,
					date_n date NULL,
					datemultirange_nn datemultirange NOT NULL,
					datemultirange_n datemultirange NULL,
					daterange_nn daterange NOT NULL,
					daterange_n daterange NULL,
					decimal_nn decimal NOT NULL,
					decimal_n decimal NULL,
					double_precision_nn double precision NOT NULL,
					double_precision_n double precision NULL,
					float4_nn float4 NOT NULL,
					float4_n float4 NULL,
					float8_nn float8 NOT NULL,
					float8_n float8 NULL,
					int_nn int NOT NULL,
					int_n int NULL,
					int2_nn int2 NOT NULL,
					int2_n int2 NULL,
					int4_nn int4 NOT NULL,
					int4_n int4 NULL,
					int8_nn int8 NOT NULL,
					int8_n int8 NULL,
					integer_nn integer NOT NULL,
					integer_n integer NULL,
					json_nn json NOT NULL,
					json_n json NULL,
					jsonb_nn jsonb NOT NULL,
					jsonb_n jsonb NULL,
					numeric_nn numeric NOT NULL,
					numeric_n numeric NULL,
					smallint_nn smallint NOT NULL,
					smallint_n smallint NULL,
					time_nn time NOT NULL,
					time_n time NULL,
					time_with_time_zone_nn time with time zone NOT NULL,
					time_with_time_zone_n time with time zone NULL,
					time_without_time_zone_nn time without time zone NOT NULL,
					time_without_time_zone_n time without time zone NULL,
					time_stamp_nn information_schema.""time_stamp"" NOT NULL,
					time_stamp_n information_schema.""time_stamp"" NULL,
					timestamp_nn timestamp NOT NULL,
					timestamp_n timestamp NULL,
					timestamp_with_time_zone_nn timestamp with time zone NOT NULL,
					timestamp_with_time_zone_n timestamp with time zone NULL,
					timestamp_without_time_zone_nn timestamp without time zone NOT NULL,
					timestamp_without_time_zone_n timestamp without time zone NULL,
					uuid_nn uuid NOT NULL,
					uuid_n uuid NULL,
					varbit_nn varbit NOT NULL,
					varbit_n varbit NULL,
					varchar_nn varchar NOT NULL,
					varchar_n varchar NULL,
					arr_bool_nn _bool NOT NULL,
					arr_bool_n _bool NULL,
					arr_float4_nn _float4 NULL,
					arr_float4_n _float4 NULL,
					arr_int2_nn _int2 NOT NULL,
					arr_int2_n _int2 NULL,
					arr_int4_nn _int4 NOT NULL,
					arr_int4_n _int4 NULL,
					arr_numeric_nn _numeric NOT NULL,
					arr_numeric_n _numeric NULL,
					arr_float8_nn _float8 NOT NULL,
					arr_float8_n _float8 NULL,
					enum_default int NOT NULL,
					enum_number int NOT NULL,
					enum_string varchar(50) NOT NULL,
					CONSTRAINT typetest_pk PRIMARY KEY (id)
    );";

        public static string insertData = @"
    INSERT INTO export.typetest (
					id,
					bool_nn,
					bool_n,
					boolean_nn,
					boolean_n,
					bigint_nn,
					bigint_n,
					bigserial_nn,
					bit_nn,
					bit_n,
					bit_varying_nn,
					bit_varing_n,
					box_nn,
					box_n,
					char_nn,
					char_n,
					char_varying_nn,
					char_varying_n,
					character_nn,
					character_n,
					character_varying_nn,
					character_varying_n,
					date_nn,
					date_n,
					datemultirange_nn,
					datemultirange_n,
					daterange_nn,
					daterange_n,
					decimal_nn,
					decimal_n,
					double_precision_nn,
					double_precision_n,
					float4_nn,
					float4_n,
					float8_nn,
					float8_n,
					int_nn,
					int_n,
					int2_nn,
					int2_n,
					int4_nn,
					int4_n,
					int8_nn,
					int8_n,
					integer_nn,
					integer_n,
					json_nn,
					json_n,
					jsonb_nn,
					jsonb_n,
					numeric_nn,
					numeric_n,
					smallint_nn,
					smallint_n,
					time_nn,
					time_n,
					time_with_time_zone_nn,
					time_with_time_zone_n,
					time_without_time_zone_nn,
					time_without_time_zone_n,
					time_stamp_nn,
					time_stamp_n,
					timestamp_nn,
					timestamp_n,
					timestamp_with_time_zone_nn,
					timestamp_with_time_zone_n,
					timestamp_without_time_zone_nn,
					timestamp_without_time_zone_n,
					uuid_nn,
					uuid_n,
					varbit_nn,
					varbit_n,
					varchar_nn,
					varchar_n,
					arr_bool_nn,
					arr_bool_n,
					arr_float4_nn,
					arr_float4_n,
					arr_int2_nn,
					arr_int2_n,
					arr_int4_nn,
					arr_int4_n,
					arr_numeric_nn,
					arr_numeric_n,
					arr_float8_nn,
					arr_float8_n,
					enum_default,
					enum_number,
					enum_string
    ) VALUES (
					1,                                      -- id
					true,                                   -- bool_nn
					false,                                  -- bool_n
					true,                                   -- boolean_nn
					false,                                  -- boolean_n
					444444,						            -- bigint_nn
					1234567893,                          -- bigint_n
					DEFAULT,                                -- bigserial_nn (use default sequence)
					B'1',                                   -- bit_nn
					B'0',                                   -- bit_n
					B'101'::varbit,                         -- bit_varying_nn
					B'10'::varbit,                          -- bit_varing_n
					'((1,2),(3,4))'::box,                   -- box_nn
					'((2,3),(4,5))'::box,                   -- box_n
					'a',                                    -- char_nn
					'b',                                    -- char_n
					'hello'::character varying,             -- char_varying_nn
					'hi'::character varying,                -- char_varying_n
					'c',                                    -- character_nn
					'd',                                    -- character_n
					'hello2'::character varying,            -- character_varying_nn
					'hi2'::character varying,               -- character_varying_n
					'2023-01-01'::date,                     -- date_nn
					'2023-02-02'::date,                     -- date_n
					'{[2023-01-01,2023-01-10)}'::datemultirange, -- datemultirange_nn
					'{[2023-02-01,2023-02-10)}'::datemultirange, -- datemultirange_n
					'[2023-03-01,2023-03-05)'::daterange,   -- daterange_nn
					'[2023-04-01,2023-04-05)'::daterange,   -- daterange_n
					12345.67,                               -- decimal_nn
					89.01,                                  -- decimal_n
					1.23456789012345,                       -- double_precision_nn
					2.34567890123456,                       -- double_precision_n
					1.23::real,                             -- float4_nn
					4.56::real,                             -- float4_n
					7.89,                                   -- float8_nn
					0.12,                                   -- float8_n
					123456,                                 -- int_nn
					654321,                                 -- int_n
					12::smallint,                           -- int2_nn
					34::smallint,                           -- int2_n
					1234,                                   -- int4_nn
					4321,                                   -- int4_n
					1234567890123,                          -- int8_nn
					3210987654321,                          -- int8_n
					42,                                     -- integer_nn
					43,                                     -- integer_n
					'{""a"":1}'::json,                      -- json_nn
					'{""b"":""text""}'::json,               -- json_n
					'{""c"":3}'::jsonb,                     -- jsonb_nn
					'{""d"":""val""}'::jsonb,               -- jsonb_n
					1000.50,                                -- numeric_nn
					2000.75,                                -- numeric_n
					2222::smallint,                        -- smallint_nn
					-368::smallint,                       -- smallint_n
					'12:34:56'::time,                       -- time_nn
					'01:02:03'::time,                       -- time_n
					'12:34:56+01'::timetz,                  -- time_with_time_zone_nn
					'01:02:03+00'::timetz,                  -- time_with_time_zone_n
					'12:00:00'::time,                       -- time_without_time_zone_nn
					'01:00:00'::time,                       -- time_without_time_zone_n
					'1975-01-01 00:00:00'::timestamp,       -- time_stamp_nn (cast to timestamp as an example)
					'1980-01-01 00:00:00'::timestamp,       -- time_stamp_n
					'2023-05-01 06:07:08'::timestamp,       -- timestamp_nn
					'2023-06-01 07:08:09'::timestamp,       -- timestamp_n
					'2023-07-01 08:09:10+00'::timestamptz,  -- timestamp_with_time_zone_nn
					'2023-08-01 09:10:11+02'::timestamptz,  -- timestamp_with_time_zone_n
					'2023-09-01 10:11:12'::timestamp,       -- timestamp_without_time_zone_nn
					'2023-10-01 11:12:13'::timestamp,       -- timestamp_without_time_zone_n
					'550e8400-e29b-41d4-a716-446655440000'::uuid, -- uuid_nn
					'550e8400-e29b-41d4-a716-446655440001'::uuid, -- uuid_n
					B'10101'::varbit,                       -- varbit_nn
					B'010'::varbit,                         -- varbit_n
					'some text'::varchar,                   -- varchar_nn
					'other text'::varchar,                  -- varchar_n
					ARRAY[true,false]::boolean[],           -- arr_bool_nn
					ARRAY[false,true]::boolean[],           -- arr_bool_n
					ARRAY[1.1,2.2]::real[],                  -- arr_float4_nn
					ARRAY[3.3,4.4]::real[],                  -- arr_float4_n
					ARRAY[1,2]::smallint[],                 -- arr_int2_nn
					ARRAY[3,4]::smallint[],                 -- arr_int2_n
					ARRAY[5,6]::integer[],                  -- arr_int4_nn
					ARRAY[7,8]::integer[],                  -- arr_int4_n
					ARRAY[9.9,10.11]::numeric[],            -- arr_numeric_nn
					ARRAY[12.12,13.13]::numeric[],          -- arr_numeric_n
					ARRAY[14.14,15.15]::float8[],           -- arr_float8_nn
					ARRAY[16.16,17.17]::float8[],           -- arr_float8_n
					1,                                      -- enum_default
					2,                                      -- enum_number
					'enum value'                            -- enum_string
    );";
    }
}
