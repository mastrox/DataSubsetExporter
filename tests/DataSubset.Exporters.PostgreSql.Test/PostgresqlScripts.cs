using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataSubset.Exporters.PostgreSql.Test
{
    internal static class PostgresqlScripts
    {
        public static string createBooleanTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        bool_nn bool NOT NULL,
        bool_n bool NULL,
        boolean_nn boolean NOT NULL,
        boolean_n boolean NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createIntegerTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        smallint_nn smallint NOT NULL,
        smallint_n smallint NULL,
        int2_nn int2 NOT NULL,
        int2_n int2 NULL,
        int_nn int NOT NULL,
        int_n int NULL,
        int4_nn int4 NOT NULL,
        int4_n int4 NULL,
        integer_nn integer NOT NULL,
        integer_n integer NULL,
        bigint_nn bigint NOT NULL,
        bigint_n bigint NULL,
        int8_nn int8 NOT NULL,
        int8_n int8 NULL,
        bigserial_nn bigserial NOT NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createBitTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        bit_nn bit NOT NULL,
        bit_n bit NULL,
        bit_varying_nn bit varying NOT NULL,
        bit_varying_n bit varying NULL,
        varbit_nn varbit NOT NULL,
        varbit_n varbit NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createGeometricTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        box_nn box NOT NULL,
        box_n box NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createCharacterTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        char_nn char NOT NULL,
        char_n char NULL,
        character_nn character NOT NULL,
        character_n character NULL,
        char_varying_nn char varying NOT NULL,
        char_varying_n char varying NULL,
        character_varying_nn character varying NOT NULL,
        character_varying_n character varying NULL,
        varchar_nn varchar NOT NULL,
        varchar_n varchar NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createDateTimeTable = @"CREATE TABLE {0}.{1} (
    id int NOT NULL,
    date_nn date NOT NULL,
    date_n date NULL,
    time_nn time NOT NULL,
    time_n time NULL,
    time_with_time_zone_nn time with time zone NOT NULL,
    time_with_time_zone_n time with time zone NULL,
    time_without_time_zone_nn time without time zone NOT NULL,
    time_without_time_zone_n time without time zone NULL,
    timestamp_nn timestamp NOT NULL,
    timestamp_n timestamp NULL,
    timestamp_with_time_zone_nn timestamp with time zone NOT NULL,
    timestamp_with_time_zone_n timestamp with time zone NULL,
    timestamp_without_time_zone_nn timestamp without time zone NOT NULL,
    timestamp_without_time_zone_n timestamp without time zone NULL,
    time_stamp_nn information_schema.""time_stamp"" NOT NULL,
    time_stamp_n information_schema.""time_stamp"" NULL,
    CONSTRAINT {1}_pk PRIMARY KEY (id)
);";

        public static string createDateTimeRangeTable = @"CREATE TABLE {0}.{1} (
    id int NOT NULL,
    daterange_nn daterange NOT NULL,
    daterange_n daterange NULL,
    datemultirange_nn datemultirange NOT NULL,
    datemultirange_n datemultirange NULL,
    CONSTRAINT {1}_pk PRIMARY KEY (id)
);";

        public static string createNumericTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        decimal_nn decimal NOT NULL,
        decimal_n decimal NULL,
        numeric_nn numeric NOT NULL,
        numeric_n numeric NULL,
        float4_nn float4 NOT NULL,
        float4_n float4 NULL,
        float8_nn float8 NOT NULL,
        float8_n float8 NULL,
        double_precision_nn double precision NOT NULL,
        double_precision_n double precision NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createJsonTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        json_nn json NOT NULL,
        json_n json NULL,
        jsonb_nn jsonb NOT NULL,
        jsonb_n jsonb NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createUuidTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        uuid_nn uuid NOT NULL,
        uuid_n uuid NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createNumericArrayTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        arr_bool_nn _bool NOT NULL,
        arr_bool_n _bool NULL,
        arr_int2_nn _int2 NOT NULL,
        arr_int2_n _int2 NULL,
        arr_int4_nn _int4 NOT NULL,
        arr_int4_n _int4 NULL,
        arr_float4_nn _float4 NOT NULL,
        arr_float4_n _float4 NULL,
        arr_float8_nn _float8 NOT NULL,
        arr_float8_n _float8 NULL,
        arr_numeric_nn _numeric NOT NULL,
        arr_numeric_n _numeric NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createEnumTable = @"CREATE TABLE {0}.{1} (
        id int NOT NULL,
        enum_default int NOT NULL,
        enum_number int NOT NULL,
        enum_string varchar(50) NOT NULL,
        CONSTRAINT {1}_pk PRIMARY KEY (id)
    );";

        public static string createDateTimeArrayTable = @"CREATE TABLE {0}.{1} (
            id int NOT NULL,
            arr_date_nn _date NOT NULL,
            arr_date_n _date NULL,
            arr_time_nn _time NOT NULL,
            arr_time_n _time NULL,
            arr_timetz_nn _timetz NOT NULL,
            arr_timetz_n _timetz NULL,
            arr_timestamp_nn _timestamp NOT NULL,
            arr_timestamp_n _timestamp NULL,
            arr_timestamptz_nn _timestamptz NOT NULL,
            arr_timestamptz_n _timestamptz NULL,
            CONSTRAINT {1}_pk PRIMARY KEY (id)
        );";


        public static string createStringArrayTable = @"CREATE TABLE {0}.{1} (
            id int NOT NULL,
            arr_char_nn _char NOT NULL,
            arr_char_n _char NULL,
            arr_varchar_nn _varchar NOT NULL,
            arr_varchar_n _varchar NULL,
            arr_text_nn _text NOT NULL,
            arr_text_n _text NULL,
            arr_bpchar_nn _bpchar NOT NULL,
            arr_bpchar_n _bpchar NULL,
            CONSTRAINT {1}_pk PRIMARY KEY (id)
        );";



        public static string insertBooleanTable = @"INSERT INTO {0}.{1} (id, bool_nn, bool_n, boolean_nn, boolean_n) VALUES (1, true, true, true, true);
                        INSERT INTO {0}.{1} (id, bool_nn, bool_n, boolean_nn, boolean_n) VALUES (2, false, true, false, true);";

        public static string insertIntegerTable = @"INSERT INTO {0}.{1} (id, smallint_nn, smallint_n, int2_nn, int2_n, int_nn, int_n, int4_nn, int4_n, integer_nn, integer_n, bigint_nn, bigint_n, int8_nn, int8_n, bigserial_nn) VALUES (1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500);
                        INSERT INTO {0}.{1} (id, smallint_nn, smallint_n, int2_nn, int2_n, int_nn, int_n, int4_nn, int4_n, integer_nn, integer_n, bigint_nn, bigint_n, int8_nn, int8_n, bigserial_nn) VALUES (2, 150, NULL, 350, NULL, 550, NULL, 750, NULL, 950, NULL, 1150, NULL, 1350, NULL, 1600);";

        public static string insertBitTable = @"INSERT INTO {0}.{1} (id, bit_nn, bit_n, bit_varying_nn, bit_varying_n, varbit_nn, varbit_n) VALUES (1, B'1', B'0', B'101', B'110', B'1010', B'1111');
                        INSERT INTO {0}.{1} (id, bit_nn, bit_n, bit_varying_nn, bit_varying_n, varbit_nn, varbit_n) VALUES (2, B'0', NULL, B'111', NULL, B'0101', NULL);";

        public static string insertGeometricTable = @"INSERT INTO {0}.{1} (id, box_nn, box_n) VALUES (1, '((1,1),(0,0))', '((2,2),(1,1))');
                        INSERT INTO {0}.{1} (id, box_nn, box_n) VALUES (2, '((3,3),(2,2))', NULL);";

        public static string insertCharacterTable = @"INSERT INTO {0}.{1} (id, char_nn, char_n, character_nn, character_n, char_varying_nn, char_varying_n, character_varying_nn, character_varying_n, varchar_nn, varchar_n) VALUES (1, 'A', 'B', 'C', 'D', 'Test1', 'Test2', 'Test3', 'Test4', 'Test5', 'Test6');
                        INSERT INTO {0}.{1} (id, char_nn, char_n, character_nn, character_n, char_varying_nn, char_varying_n, character_varying_nn, character_varying_n, varchar_nn, varchar_n) VALUES (2, 'X', NULL, 'Y', NULL, 'Sample1', NULL, 'Sample2', NULL, 'Sample3', NULL);";

        public static string insertDateTimeTable = @"INSERT INTO {0}.{1} (id, date_nn, date_n, time_nn, time_n, time_with_time_zone_nn, time_with_time_zone_n, time_without_time_zone_nn, time_without_time_zone_n, timestamp_nn, timestamp_n, timestamp_with_time_zone_nn, timestamp_with_time_zone_n, timestamp_without_time_zone_nn, timestamp_without_time_zone_n, time_stamp_nn, time_stamp_n) VALUES (1, '2023-01-01', '2023-02-01', '12:30:00', '13:45:00', '14:00:00+01', '15:30:00+01', '16:00:00', '17:15:00', '2023-01-01 12:00:00', '2023-02-01 13:00:00', '2023-01-01 14:00:00+01', '2023-02-01 15:00:00+01', '2023-01-01 16:00:00', '2023-02-01 17:00:00', '2023-01-01 18:00:00', '2023-02-01 19:00:00');
                INSERT INTO {0}.{1} (id, date_nn, date_n, time_nn, time_n, time_with_time_zone_nn, time_with_time_zone_n, time_without_time_zone_nn, time_without_time_zone_n, timestamp_nn, timestamp_n, timestamp_with_time_zone_nn, timestamp_with_time_zone_n, timestamp_without_time_zone_nn, timestamp_without_time_zone_n, time_stamp_nn, time_stamp_n) VALUES (2, '2023-03-01', NULL, '18:00:00', NULL, '19:00:00+01', NULL, '20:00:00', NULL, '2023-03-01 10:00:00', NULL, '2023-03-01 11:00:00+01', NULL, '2023-03-01 12:00:00', NULL, '2023-03-01 13:00:00', NULL);";

        public static string insertDateTimeRangeTable = @"INSERT INTO {0}.{1} (id, daterange_nn, daterange_n, datemultirange_nn, datemultirange_n) VALUES (1, '[2023-01-01,2023-01-31]', '[2023-02-01,2023-02-28]', '{{[2023-03-01,2023-03-31]}}', '{{[2023-04-01,2023-04-30], [2024-05-01,2024-05-30]}}');
                INSERT INTO {0}.{1} (id, daterange_nn, daterange_n, datemultirange_nn, datemultirange_n) VALUES (2, '[2023-05-01,2023-05-31]', NULL, '{{[2023-06-01,2023-06-30]}}', NULL);";

        public static string insertNumericTable = @"INSERT INTO {0}.{1} (id, decimal_nn, decimal_n, numeric_nn, numeric_n, float4_nn, float4_n, float8_nn, float8_n, double_precision_nn, double_precision_n) VALUES (1, 123.45, 678.90, 111.22, 333.44, 1.23, 4.56, 7.89, 10.11, 12.34, 56.78);
                        INSERT INTO {0}.{1} (id, decimal_nn, decimal_n, numeric_nn, numeric_n, float4_nn, float4_n, float8_nn, float8_n, double_precision_nn, double_precision_n) VALUES (2, 999.99, NULL, 888.88, NULL, 2.34, NULL, 5.67, NULL, 8.90, NULL);";

      public static string insertJsonTable = @"INSERT INTO {0}.{1} (id, json_nn, json_n, jsonb_nn, jsonb_n) VALUES (1, '{""name"":""John"",""age"":30}', '{""city"":""New York""}', '{""country"":""USA""}', '{""state"":""NY""}');
                        INSERT INTO {0}.{1} (id, json_nn, json_n, jsonb_nn, jsonb_n) VALUES (2, '{""name"":""Jane"",""age"":25}', NULL, '{""country"":""Canada""}', NULL);";

        public static string insertUuidTable = @"INSERT INTO {0}.{1} (id, uuid_nn, uuid_n) VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'b1ffcd99-8d1c-5fg9-cc7e-7cc0ce491b22');
                INSERT INTO {0}.{1} (id, uuid_nn, uuid_n) VALUES (2, 'c2ggde99-7e2d-6hg0-dd8f-8dd1df502c33', 'd3hhef99-6f3e-7ih1-ee9g-9ee2eg613d44');";

        public static string insertNumericArrayTable = @"INSERT INTO {0}.{1} (id, arr_bool_nn, arr_bool_n, arr_int2_nn, arr_int2_n, arr_int4_nn, arr_int4_n, arr_float4_nn, arr_float4_n, arr_float8_nn, arr_float8_n, arr_numeric_nn, arr_numeric_n) VALUES (1, '{{true, false, true}}', '{{false, true}}', '{{1, 2, 3}}', '{{10, 20}}', '{{100, 200, 300}}', '{{1000, 2000}}', '{{1.1, 2.2, 3.3}}', '{{10.5, 20.5}}', '{{100.123, 200.456}}', '{{1000.789}}', '{{123.45, 678.90}}', '{{999.99, 111.11}}');
                INSERT INTO {0}.{1} (id, arr_bool_nn, arr_bool_n, arr_int2_nn, arr_int2_n, arr_int4_nn, arr_int4_n, arr_float4_nn, arr_float4_n, arr_float8_nn, arr_float8_n, arr_numeric_nn, arr_numeric_n) VALUES (2, '{{false}}', '{{true, true}}', '{{5}}', '{{15, 25}}', '{{500}}', '{{1500, 2500}}', '{{5.5}}', '{{15.5, 25.5}}', '{{500.5}}', '{{1500.5, 2500.5}}', '{{555.55}}', '{{666.66, 777.77}}');";

        public static string insertEnumTable = @"INSERT INTO {0}.{1} (id, enum_default, enum_number, enum_string) VALUES (1, 0, 1, 'Active');
                INSERT INTO {0}.{1} (id, enum_default, enum_number, enum_string) VALUES (2, 1, 2, 'Inactive');
                INSERT INTO {0}.{1} (id, enum_default, enum_number, enum_string) VALUES (3, 2, 3, 'Pending');";


        public static string insertDateTimeArrayTable = @"INSERT INTO {0}.{1} (id, arr_date_nn, arr_date_n, arr_time_nn, arr_time_n, arr_timetz_nn, arr_timetz_n, arr_timestamp_nn, arr_timestamp_n, arr_timestamptz_nn, arr_timestamptz_n) VALUES (1, '{{""2023-01-01"", ""2023-02-01"", ""2023-03-01""}}', '{{""2023-04-01"", ""2023-05-01""}}', '{{""12:30:00"", ""13:45:00"", ""14:00:00""}}', '{{""15:30:00"", ""16:45:00""}}', '{{""14:00:00+01"", ""15:30:00+01""}}', '{{""16:00:00+01"", ""17:30:00+01""}}', '{{""2023-01-01 12:00:00"", ""2023-02-01 13:00:00""}}', '{{""2023-03-01 14:00:00"", ""2023-04-01 15:00:00""}}', '{{""2023-01-01 14:00:00+01"", ""2023-02-01 15:00:00+01""}}', '{{""2023-03-01 16:00:00+01"", ""2023-04-01 17:00:00+01""}}');
                INSERT INTO {0}.{1} (id, arr_date_nn, arr_date_n, arr_time_nn, arr_time_n, arr_timetz_nn, arr_timetz_n, arr_timestamp_nn, arr_timestamp_n, arr_timestamptz_nn, arr_timestamptz_n) VALUES (2, '{{""2023-06-01""}}', '{{""2023-07-01"", ""2023-08-01""}}', '{{""18:00:00""}}', '{{""19:00:00"", ""20:00:00""}}', '{{""18:00:00+01""}}', '{{""19:00:00+01"", ""20:00:00+01""}}', '{{""2023-05-01 10:00:00""}}', '{{""2023-06-01 11:00:00"", ""2023-07-01 12:00:00""}}', '{{""2023-05-01 11:00:00+01""}}', '{{""2023-06-01 12:00:00+01"", ""2023-07-01 13:00:00+01""}}');";
        

        public static string insertStringArrayTable = @"INSERT INTO {0}.{1} (id, arr_char_nn, arr_char_n, arr_varchar_nn, arr_varchar_n, arr_text_nn, arr_text_n, arr_bpchar_nn, arr_bpchar_n) VALUES (1, '{{""A"", ""B"", ""C""}}', '{{""D"", ""E""}}', '{{""Test1"", ""Test2"", ""Test3""}}', '{{""Sample1"", ""Sample2""}}', '{{""Long text 1"", ""Long text 2""}}', '{{""Text 3"", ""Text 4""}}', '{{""X"", ""Y"", ""Z""}}', '{{""P"", ""Q""}}');
        INSERT INTO {0}.{1} (id, arr_char_nn, arr_char_n, arr_varchar_nn, arr_varchar_n, arr_text_nn, arr_text_n, arr_bpchar_nn, arr_bpchar_n) VALUES (2, '{{""M""}}', '{{""N"", ""O""}}', '{{""Value1""}}', '{{""Value2"", ""Value3""}}', '{{""Description""}}', '{{""Info1"", ""Info2""}}', '{{""K""}}', '{{""L"", ""M""}}');";
    }


}
