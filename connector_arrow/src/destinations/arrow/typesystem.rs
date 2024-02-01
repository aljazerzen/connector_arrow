use crate::impl_typesystem;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// Analogous to [arrow::datatypes::DataType]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ArrowTypeSystem {
    // /// Null type
    // Null(bool),
    /// A boolean datatype representing the values `true` and `false`.
    Boolean(bool),

    // /// A signed 8-bit integer.
    // Int8(bool),

    // /// A signed 16-bit integer.
    // Int16(bool),
    /// A signed 32-bit integer.
    Int32(bool),

    /// A signed 64-bit integer.
    Int64(bool),

    // /// An unsigned 8-bit integer.
    // UInt8(bool),

    // /// An unsigned 16-bit integer.
    // UInt16(bool),
    /// An unsigned 32-bit integer.
    UInt32(bool),

    /// An unsigned 64-bit integer.
    UInt64(bool),

    // /// A 16-bit floating point number.
    // Float16(bool),
    /// A 32-bit floating point number.
    Float32(bool),

    /// A 64-bit floating point number.
    Float64(bool),

    /// ISO 8601 combined date and time with time zone.
    ///
    /// Note: this is supposed to be analogous to [arrow::datatypes::DataType::Timestamp].
    // TODO: this should be changed to TimestampTz
    DateTimeTz(bool),

    /// A signed 32-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date32(bool),

    /// A signed 64-bit date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in milliseconds (64 bits). Values are evenly divisible by 86400000.
    Date64(bool),

    // /// A signed 32-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    // /// Must be either seconds or milliseconds.
    // Time32(bool),
    /// A signed 64-bit time representing the elapsed microseconds since midnight in the unit of `TimeUnit`.
    /// Note: [arrow::datatypes::DataType::Time64] type can also contain elapsed nanoseconds)
    Time64(bool),

    // /// Measure of elapsed time in either seconds, milliseconds, microseconds or nanoseconds.
    // Duration(bool)(TimeUnit),

    // /// A "calendar" interval which models types that don't necessarily
    // /// have a precise duration without the context of a base timestamp (e.g.
    // /// days can differ in length during day light savings time transitions).
    // Interval(bool)(IntervalUnit),

    // /// Opaque binary data of variable length.
    // ///
    // /// A single Binary array can store up to [`i32::MAX`] bytes
    // /// of binary data in total.
    // Binary(bool),

    // /// Opaque binary data of fixed size.
    // /// Enum parameter specifies the number of bytes per value.
    // FixedSizeBinary(bool)(i32),
    /// Opaque binary data of variable length and 64-bit offsets.
    ///
    /// A single LargeBinary array can store up to [`i64::MAX`] bytes
    /// of binary data in total.
    LargeBinary(bool),

    // /// A variable-length string in Unicode with UTF-8 encoding.
    // ///
    // /// A single Utf8 array can store up to [`i32::MAX`] bytes
    // /// of string data in total.
    // Utf8(bool),
    /// A variable-length string in Unicode with UFT-8 encoding and 64-bit offsets.
    ///
    /// A single LargeUtf8 array can store up to [`i64::MAX`] bytes
    /// of string data in total.
    LargeUtf8(bool),
    // /// A list of some logical data type with variable length.
    // ///
    // /// A single List array can store up to [`i32::MAX`] elements in total.
    // List(bool)(FieldRef),

    // /// A list of some logical data type with fixed length.
    // FixedSizeList(bool)(FieldRef, i32),

    // /// A list of some logical data type with variable length and 64-bit offsets.
    // ///
    // /// A single LargeList array can store up to [`i64::MAX`] elements in total.
    // LargeList(bool)(FieldRef),

    // /// A nested datatype that contains a number of sub-fields.
    // Struct(bool)(Fields),

    // /// A nested datatype that can represent slots of differing types. Components:
    // ///
    // /// 1. [`UnionFields`]
    // /// 2. The type of union (Sparse or Dense)
    // Union(bool)(UnionFields, UnionMode),

    // /// A dictionary encoded array (`key_type`, `value_type`), where
    // /// each array element is an index of `key_type` into an
    // /// associated dictionary of `value_type`.
    // ///
    // /// Dictionary arrays are used to store columns of `value_type`
    // /// that contain many repeated values using less memory, but with
    // /// a higher CPU overhead for some operations.
    // ///
    // /// This type mostly used to represent low cardinality string
    // /// arrays or a limited set of primitive types as integers.
    // Dictionary(bool)(Box<DataType>, Box<DataType>),

    // /// Exact 128-bit width decimal value with precision and scale
    // ///
    // /// * precision is the total number of digits
    // /// * scale is the number of digits past the decimal
    // ///
    // /// For example the number 123.45 has precision 5 and scale 2.
    // ///
    // /// In certain situations, scale could be negative number. For
    // /// negative scale, it is the number of padding 0 to the right
    // /// of the digits.
    // ///
    // /// For example the number 12300 could be treated as a decimal
    // /// has precision 3 and scale -2.
    // Decimal128(bool)(u8, i8),

    // /// Exact 256-bit width decimal value with precision and scale
    // ///
    // /// * precision is the total number of digits
    // /// * scale is the number of digits past the decimal
    // ///
    // /// For example the number 123.45 has precision 5 and scale 2.
    // ///
    // /// In certain situations, scale could be negative number. For
    // /// negative scale, it is the number of padding 0 to the right
    // /// of the digits.
    // ///
    // /// For example the number 12300 could be treated as a decimal
    // /// has precision 3 and scale -2.
    // Decimal256(bool)(u8, i8),

    // /// A Map is a logical nested type that is represented as
    // ///
    // /// `List<entries: Struct<key: K, value: V>>`
    // ///
    // /// The keys and values are each respectively contiguous.
    // /// The key and value types are not constrained, but keys should be
    // /// hashable and unique.
    // /// Whether the keys are sorted can be set in the `bool` after the `Field`.
    // ///
    // /// In a field with Map type, the field has a child Struct field, which then
    // /// has two children: key type and the second the value type. The names of the
    // /// child fields may be respectively "entries", "key", and "value", but this is
    // /// not enforced.
    // Map(bool)(FieldRef, bool),

    // /// A run-end encoding (REE) is a variation of run-length encoding (RLE). These
    // /// encodings are well-suited for representing data containing sequences of the
    // /// same value, called runs. Each run is represented as a value and an integer giving
    // /// the index in the array where the run ends.
    // ///
    // /// A run-end encoded array has no buffers by itself, but has two child arrays. The
    // /// first child array, called the run ends array, holds either 16, 32, or 64-bit
    // /// signed integers. The actual values of each run are held in the second child array.
    // ///
    // /// These child arrays are prescribed the standard names of "run_ends" and "values"
    // /// respectively.
    // RunEndEncoded(bool)(FieldRef, FieldRef),
}

impl_typesystem! {
    system = ArrowTypeSystem,
    mappings = {
        { Boolean     => bool          }
        { Int32       => i32           }
        { Int64       => i64           }
        { UInt32      => u32           }
        { UInt64      => u64           }
        { Float32     => f32           }
        { Float64     => f64           }
        { Time64      => NaiveTime     }
        { Date32      => NaiveDate     }
        { Date64      => NaiveDateTime }
        { DateTimeTz  => DateTime<Utc> }
        { LargeUtf8   => String        }
        { LargeBinary => Vec<u8>       }
    }
}
