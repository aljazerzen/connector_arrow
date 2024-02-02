use arrow::datatypes::{DataType, Field};

/// Take a value of type `ty` from [Produce] and insert it into [Consume].
pub fn transport<'r, P: Produce<'r>, C: Consume>(f: &Field, src: &P, dst: &mut C) {
    log::debug!("transporting value of type {f:?}");

    // TODO: connector-x goes a step further here: instead of having this match in the hot path,
    // this function returns a transporter function that is stored in a vec, for faster lookup.

    match f.data_type() {
        DataType::Boolean if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<bool>(src));
        }
        DataType::Boolean => {
            Consume::consume(dst, Produce::produce::<Option<bool>>(src));
        }
        DataType::Int64 if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<i64>(src));
        }
        DataType::Int64 => {
            Consume::consume(dst, Produce::produce::<Option<i64>>(src));
        }
        DataType::Int32 if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<i32>(src));
        }
        DataType::Int32 => {
            Consume::consume(dst, Produce::produce::<Option<i32>>(src));
        }
        DataType::Int8 if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<i8>(src));
        }
        DataType::Int8 => {
            Consume::consume(dst, Produce::produce::<Option<i8>>(src));
        }
        DataType::LargeUtf8 if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<String>(src));
        }
        DataType::LargeUtf8 => {
            Consume::consume(dst, Produce::produce::<Option<String>>(src));
        }
        DataType::Float64 if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<f64>(src));
        }
        DataType::Float64 => {
            Consume::consume(dst, Produce::produce::<Option<f64>>(src));
        }
        DataType::LargeBinary if !f.is_nullable() => {
            Consume::consume(dst, Produce::produce::<Vec<u8>>(src));
        }
        DataType::LargeBinary => {
            Consume::consume(dst, Produce::produce::<Option<Vec<u8>>>(src));
        }
        _ => todo!("unimplemented transport of {:?}", f.data_type()),
    }
}

pub trait Produce<'r>:
    ProduceTy<'r, bool>
    + ProduceTy<'r, i64>
    + ProduceTy<'r, i32>
    + ProduceTy<'r, i16>
    + ProduceTy<'r, i8>
    + ProduceTy<'r, String>
    + ProduceTy<'r, f64>
    + ProduceTy<'r, Vec<u8>>
    + ProduceTy<'r, Option<bool>>
    + ProduceTy<'r, Option<i64>>
    + ProduceTy<'r, Option<i32>>
    + ProduceTy<'r, Option<i16>>
    + ProduceTy<'r, Option<i8>>
    + ProduceTy<'r, Option<String>>
    + ProduceTy<'r, Option<f64>>
    + ProduceTy<'r, Option<Vec<u8>>>
{
    fn produce<T>(&self) -> T
    where
        Self: ProduceTy<'r, T>,
    {
        ProduceTy::produce(self)
    }
}

pub trait ProduceTy<'r, T> {
    fn produce(&self) -> T;
}

pub trait Consume:
    ConsumeTy<bool>
    + ConsumeTy<i64>
    + ConsumeTy<i32>
    + ConsumeTy<i16>
    + ConsumeTy<i8>
    + ConsumeTy<String>
    + ConsumeTy<f64>
    + ConsumeTy<Vec<u8>>
    + ConsumeTy<Option<bool>>
    + ConsumeTy<Option<i64>>
    + ConsumeTy<Option<i32>>
    + ConsumeTy<Option<i16>>
    + ConsumeTy<Option<i8>>
    + ConsumeTy<Option<String>>
    + ConsumeTy<Option<f64>>
    + ConsumeTy<Option<Vec<u8>>>
{
    fn consume<T>(&mut self, value: T)
    where
        Self: ConsumeTy<T>,
    {
        ConsumeTy::consume(self, value)
    }
}

pub trait ConsumeTy<T> {
    fn consume(&mut self, value: T);
}

pub mod print {
    use super::{Consume, ConsumeTy};

    pub struct PrintConsumer();

    impl Consume for PrintConsumer {}

    impl<T: std::fmt::Debug> ConsumeTy<T> for PrintConsumer {
        fn consume(&mut self, value: T) {
            println!("bool: {value:?}");
        }
    }
}
