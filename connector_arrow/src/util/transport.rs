use arrow::datatypes::{DataType, Field};

use crate::errors::ConnectorError;

/// Take a value of type `ty` from [Produce] and insert it into [Consume].
pub fn transport<'r, P: Produce<'r>, C: Consume>(
    f: &Field,
    p: P,
    c: &mut C,
) -> Result<(), ConnectorError> {
    log::debug!("transporting value of type {f:?}");

    // TODO: connector-x goes a step further here: instead of having this match in the hot path,
    // this function returns a transporter function that is stored in a vec, for faster lookup.

    use DataType::*;

    if !f.is_nullable() {
        match f.data_type() {
            Null => c.consume(()),
            Boolean => c.consume(ProduceTy::<bool>::produce(p)?),
            Int8 => c.consume(ProduceTy::<i8>::produce(p)?),
            Int16 => c.consume(ProduceTy::<i16>::produce(p)?),
            Int32 => c.consume(ProduceTy::<i32>::produce(p)?),
            Int64 => c.consume(ProduceTy::<i64>::produce(p)?),
            UInt8 => c.consume(ProduceTy::<u8>::produce(p)?),
            UInt16 => c.consume(ProduceTy::<u16>::produce(p)?),
            UInt32 => c.consume(ProduceTy::<u32>::produce(p)?),
            UInt64 => c.consume(ProduceTy::<u64>::produce(p)?),
            Float32 => c.consume(ProduceTy::<f32>::produce(p)?),
            Float64 => c.consume(ProduceTy::<f64>::produce(p)?),
            Binary | LargeBinary => c.consume(ProduceTy::<Vec<u8>>::produce(p)?),
            Utf8 | LargeUtf8 => c.consume(ProduceTy::<String>::produce(p)?),
            _ => todo!("unimplemented transport of {:?}", f.data_type()),
        }
    } else {
        match f.data_type() {
            Null => c.consume_opt(Some(())),
            Boolean => c.consume_opt(ProduceTy::<bool>::produce_opt(p)?),
            Int8 => c.consume_opt(ProduceTy::<i8>::produce_opt(p)?),
            Int16 => c.consume_opt(ProduceTy::<i16>::produce_opt(p)?),
            Int32 => c.consume_opt(ProduceTy::<i32>::produce_opt(p)?),
            Int64 => c.consume_opt(ProduceTy::<i64>::produce_opt(p)?),
            UInt8 => c.consume_opt(ProduceTy::<u8>::produce_opt(p)?),
            UInt16 => c.consume_opt(ProduceTy::<u16>::produce_opt(p)?),
            UInt32 => c.consume_opt(ProduceTy::<u32>::produce_opt(p)?),
            UInt64 => c.consume_opt(ProduceTy::<u64>::produce_opt(p)?),
            Float32 => c.consume_opt(ProduceTy::<f32>::produce_opt(p)?),
            Float64 => c.consume_opt(ProduceTy::<f64>::produce_opt(p)?),
            Binary | LargeBinary => c.consume_opt(ProduceTy::<Vec<u8>>::produce_opt(p)?),
            Utf8 | LargeUtf8 => c.consume_opt(ProduceTy::<String>::produce_opt(p)?),
            _ => todo!("unimplemented transport of {:?}", f.data_type()),
        }
    }
    Ok(())
}

pub trait Produce<'r>:
    ProduceTy<'r, bool>
    + ProduceTy<'r, i8>
    + ProduceTy<'r, i16>
    + ProduceTy<'r, i32>
    + ProduceTy<'r, i64>
    + ProduceTy<'r, u8>
    + ProduceTy<'r, u16>
    + ProduceTy<'r, u32>
    + ProduceTy<'r, u64>
    + ProduceTy<'r, f32>
    + ProduceTy<'r, f64>
    + ProduceTy<'r, Vec<u8>>
    + ProduceTy<'r, String>
{
}

pub trait ProduceTy<'r, T> {
    fn produce(self) -> Result<T, ConnectorError>;

    fn produce_opt(self) -> Result<Option<T>, ConnectorError>;
}

pub trait Consume:
    ConsumeTy<()>
    + ConsumeTy<bool>
    + ConsumeTy<i8>
    + ConsumeTy<i16>
    + ConsumeTy<i32>
    + ConsumeTy<i64>
    + ConsumeTy<u8>
    + ConsumeTy<u16>
    + ConsumeTy<u32>
    + ConsumeTy<u64>
    + ConsumeTy<f32>
    + ConsumeTy<f64>
    + ConsumeTy<Vec<u8>>
    + ConsumeTy<String>
{
}

pub trait ConsumeTy<T> {
    fn consume(&mut self, value: T);
    fn consume_opt(&mut self, value: Option<T>);
}

pub mod print {
    use super::{Consume, ConsumeTy};

    pub struct PrintConsumer();

    impl Consume for PrintConsumer {}

    impl<T: std::fmt::Debug> ConsumeTy<T> for PrintConsumer {
        fn consume(&mut self, value: T) {
            println!("{}: {value:?}", std::any::type_name::<T>());
        }
        fn consume_opt(&mut self, value: Option<T>) {
            println!("{}: {value:?}", std::any::type_name::<T>());
        }
    }
}
