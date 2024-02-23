// Adapted from rust_decimal

use arrow::{array::ArrowNativeTypeOp, datatypes::i256};
use bytes::{BufMut, BytesMut};
use itertools::Itertools;
use std::io::{Cursor, Read};

fn read_two_bytes(cursor: &mut Cursor<&[u8]>) -> std::io::Result<[u8; 2]> {
    let mut result = [0; 2];
    cursor.read_exact(&mut result)?;
    Ok(result)
}

// Decimals are represented as follows:
//  u16 numGroups
//  i16 weightFirstGroup (10000^weight)
//  u16 sign (0x0000 = positive, 0x4000 = negative, 0xC000 = NaN)
//  i16 dscale. Number of digits (in base 10) to print after decimal separator
//  u16 group value (repeated numGroups times)

// So if we were to take the number: 3950.123456
//
//  Stored on Disk:
//    00 03 00 00 00 00 00 06 0F 6E 04 D2 15 E0
//
//  Number of groups: 00 03
//  Weight of first group: 00 00
//  Sign: 00 00
//  DScale: 00 06
//
// 0F 6E = 3950
//   result = result + 3950 * 1;
// 04 D2 = 1234
//   result = result + 1234 * 0.0001;
// 15 E0 = 5600
//   result = result + 5600 * 0.00000001;

pub fn from_sql(raw: &[u8]) -> std::io::Result<String> {
    let mut raw = Cursor::new(raw);
    let num_groups = u16::from_be_bytes(read_two_bytes(&mut raw)?);
    // 10000^weight
    let weight_first = i16::from_be_bytes(read_two_bytes(&mut raw)?);
    // sign: 0x0000 = positive, 0x4000 = negative, 0xC000 = NaN
    let sign = u16::from_be_bytes(read_two_bytes(&mut raw)?);
    // number of digits (in base 10) to print after decimal separator
    let scale = i16::from_be_bytes(read_two_bytes(&mut raw)?);

    let negate = match sign {
        0x0000 => false,
        0x4000 => true,
        0xD000 => return Ok("Infinity".into()),
        0xF000 => return Ok("-Infinity".into()),
        0xC000 => return Ok("NaN".into()),
        _ => panic!("invalid sign value: {:x}", sign),
    };

    // read all of the groups
    let mut buf = String::new();
    let mut prev_remainder = 0;
    for _ in 0..num_groups as usize {
        let group = u16::from_be_bytes(read_two_bytes(&mut raw)?);
        let number = group as u32 + prev_remainder * 1_0000;

        buf += &(format!("{:0>4}", (number / 10)));
        prev_remainder = number % 10;
    }
    buf += &prev_remainder.to_string();

    // find relevant slices
    let dot = (weight_first * 4 + 5) as usize;
    let frac_end = dot + scale as usize;
    if frac_end > buf.len() {
        buf += &"0".repeat(frac_end - buf.len());
    }
    let (first_non_zero, _) = buf
        .chars()
        .find_position(|x| *x != '0')
        .unwrap_or((dot - 1, '0'));
    let int_start = first_non_zero.min(dot - 1);

    // compose result
    let mut res = String::new();
    if negate {
        res += "-";
    }
    res += &buf[int_start..dot];

    if scale > 0 {
        res += ".";
        res += &buf[dot..frac_end];
    }
    Ok(res)
}

pub fn i128_to_sql(data: i128, scale: i8, out: &mut BytesMut) {
    let neg = data < 0;

    let mut data = if neg { data.wrapping_neg() } else { data } as u128;

    let mut groups = Vec::with_capacity(8);

    let scale_offset = scale % 4;
    let mut weight = -(scale as i16) / 4 - 1;

    // do first group
    if scale_offset > 0 {
        let multiplier = 10u128.pow(scale_offset as u32);
        groups.push((data % multiplier) as u16 * 10u16.pow(4 - scale_offset as u32));
        data /= multiplier;
    }

    // do all other groups
    while data != 0 {
        groups.push((data % 10000) as u16);
        data /= 10000;
        weight += 1;
    }
    groups.reverse();

    let num_groups = groups.len();

    // Reserve bytes
    out.reserve(8 + num_groups * 2);

    // Number of groups
    out.put_i16(num_groups as i16);
    // Weight of first group
    out.put_i16(weight);
    // Sign
    out.put_i16(if neg { 0x4000 } else { 0x0000 });
    // DScale
    out.put_i16(scale as i16);
    // Now process the number
    for group in groups {
        out.put_u16(group);
    }
}

pub fn i256_to_sql(data: i256, scale: i8, out: &mut BytesMut) {
    let neg = data.is_negative();

    let mut data = if neg { data.wrapping_neg() } else { data };

    let mut groups = Vec::with_capacity(8);

    let scale_offset = scale % 4;
    let mut weight = -(scale as i16) / 4 - 1;

    // do first group
    if scale_offset > 0 {
        let multiplier = i256::from_i128(10)
            .pow_checked(scale_offset as u32)
            .unwrap();
        let group = (data.wrapping_rem(multiplier)).as_i128() as u16;
        groups.push(group * 10u16.pow(4 - scale_offset as u32));
        data = data.div_wrapping(multiplier);
    }

    // do all other groups
    while !data.is_zero() {
        let multiplier = i256::from_i128(10000);
        let group = (data.wrapping_rem(multiplier)).as_i128() as u16;
        groups.push(group);
        data = data.wrapping_div(multiplier);
        weight += 1;
    }
    groups.reverse();

    let num_groups = groups.len();

    if num_groups > 100 {
        panic!();
    }

    // Reserve bytes
    out.reserve(8 + num_groups * 2);

    // Number of groups
    out.put_i16(num_groups as i16);
    // Weight of first group
    out.put_i16(weight);
    // Sign
    out.put_i16(if neg { 0x4000 } else { 0x0000 });
    // DScale
    out.put_i16(scale as i16);
    // Now process the number
    for group in groups {
        out.put_u16(group);
    }
}

#[test]
fn test_from_sql_01() {
    let raw = [
        0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x0F, 0x6E, 0x04, 0xD2, 0x15, 0xE0,
    ];

    let res = (3950123456i128, 6);
    assert_eq!(&from_sql(&raw).unwrap(), "3950.123456");

    let mut bytes = BytesMut::new();
    i128_to_sql(res.0, res.1 as i8, &mut bytes);
    assert_eq!(&raw, &bytes[..]);
}

#[test]
fn test_from_sql_02() {
    let raw = [
        0x00, 0x03, 0x00, 0x00, 0x40, 0x00, 0x00, 0x06, 0x0F, 0x6E, 0x04, 0xD2, 0x15, 0xE0,
    ];

    let res = (-3950123456i128, 6);

    assert_eq!(&from_sql(&raw).unwrap(), "-3950.123456");

    let mut bytes = BytesMut::new();
    i128_to_sql(res.0, res.1 as i8, &mut bytes);
    assert_eq!(&raw, &bytes[..]);
}

#[test]
fn test_from_sql_03() {
    let raw = [
        0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x0F, 0x6E, 0x04, 0xD2, 0x15, 0xE0,
    ];

    let res = (39501234560i128, 7);

    assert_eq!(&from_sql(&raw).unwrap(), "3950.1234560");

    let mut bytes = BytesMut::new();
    i128_to_sql(res.0, res.1 as i8, &mut bytes);
    assert_eq!(&raw, &bytes[..]);
}
