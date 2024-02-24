// List of all PostgreSQL 16 types:
// boolean
/*
SELECT * FROM
    (SELECT
        CAST(false as bool) as bool_l,
        CAST(true as bool) as bool_h
    ),
    (SELECT
        CAST(-32768 as smallint) as smallint_l,
        CAST(32767 as smallint) as smallint_h
    ),
    (SELECT
        CAST(-2147483648 as integer) as integer_l,
        CAST(2147483647 as integer) as integer_h
    ),
    (SELECT
        CAST(-9223372036854775808 as bigint) as bigint_l,
        CAST(9223372036854775807 as bigint) as bigint_h
    ),
    (SELECT
        CAST(0.0000000000000001 as real) as real_l,
        CAST(320408189701000883874270966773636421857.343 as real) as real_h
    ),
    (SELECT
        CAST(0.0000000000000001 as double precision) as double_precision_l,
        CAST(169887091512878784072456977815639144986844791761391463199488195613960270320306312503565450353892964007290987523985546822219622743205157680656345163189398898603771312392646033647881441914598093884681267521417553845725875093723153981243150747360440575022991266174916573292797139518035888312258153780424083020663.466 as double precision) as double_precision_h
    )
 */

// numeric [ (p, s) ]

// bit [ (n) ]
// bit varying [ (n) ]
// bytea

// character [ (n) ]
// character varying [ (n) ]
// text

// point
// box
// circle
// line
// lseg
// polygon
// path

// timestamp [ (p) ] [ without time zone ]
// timestamp [ (p) ] with time zone
// date
// time [ (p) ] [ without time zone ]
// time [ (p) ] with time zone
// interval [ fields ] [ (p) ]

// cidr
// inet
// macaddr
// macaddr8
// money

// json
// jsonb
// xml
// uuid

// tsquery
// tsvector

// pg_lsn
// pg_snapshot
// txid_snapshot
