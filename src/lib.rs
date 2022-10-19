use std::io::{Cursor, Error, Write};
use byteorder::{ReadBytesExt, WriteBytesExt};

pub mod tree;
pub mod node;

pub trait Size {
    fn size() -> u64;
}

pub trait EncodableU8 {
    //可能会很大
    fn encode(&self, buf: &mut Vec<u8>) -> Result<u64, Error>;
}

pub trait DecodableU8 where Self: Sized {
    fn decode(buf: &[u8]) -> Result<(Self, u64), Error>;
}


use byteorder::{BigEndian};

#[derive(Debug)]
pub struct ValueTest {
    pub id: u32,
    pub data: String,
}

impl Size for u64 {
    fn size() -> u64 {
        std::mem::size_of::<u64>() as u64
    }
}

impl EncodableU8 for u64 {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<u64, Error> {
        buf.write_u64::<BigEndian>(*self)?;
        Ok(buf.len() as u64)
    }
}

impl DecodableU8 for u64 {
    fn decode(buf: &[u8]) -> Result<(Self, u64), Error> {
        let  data = Cursor::new(&buf[0..8]).read_u64::<BigEndian>()?;
        Ok((data, 8))
    }
}

impl EncodableU8 for ValueTest {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<u64, Error> {
        if let Err(e) = buf.write_u32::<BigEndian>(self.id) {
            return Err(e);
        }

        if let Err(e) = buf.write(self.data.as_bytes()) {
            return Err(e);
        }
        Ok(buf.len() as u64)
    }
}

impl DecodableU8 for ValueTest {
    fn decode(buf: &[u8]) -> Result<(Self, u64), Error> {
        let mut id = 0;
        if let Ok(data) = Cursor::new(&buf[0..4]).read_u32::<BigEndian>() {
            id = data;
        }
        let data = String::from_utf8_lossy(&buf[4..]).to_string();
        let len = data.len() as u64;
        Ok((Self {
            id,
            data,
        }, 4 + len))
    }
}


/// |u8 (00000000-》1有效  10 root位置 100 节点位置  1000叶子位置  10000额外动态数据位)| u64 key个数| u64数据个数 |u64 可存储数据大小| u64 写一个节点位置| u64上一个节点位置|
/// 数据部分  |u64 数据实际长度(包括key) | key |数据限制长度+u64数据页位置|  or  |u64 偏移|key | u64偏移|...采用 wiki B+tree 实现方式
// 是否要把页的大小写进去？是否还需要标记位标记数据是否是动态长度？？(大部分多部分常考 MySQL B+Tree)
// 1.数据需要实现 decode encode trait; 先编写(泛型参数key和value) LRU 做页、空闲页、数据页、缓存
// 2.搜索(最小值，最大值，范围，精确单条 多条) 与 插入规则
// 3.删除规则 ---> rust 重新以前 golang 写的小工具 数据持久化从 csv 变成B+树
// 4.重新构建整个b+树，(删除空洞)
// 5.支持线程安全
#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};
    use byteorder::ReadBytesExt;
    use crate::{DecodableU8, EncodableU8, ValueTest};
    use std::fs::OpenOptions;
    use std::io::{Seek, SeekFrom};
    use byteorder::{BigEndian, WriteBytesExt};

    #[test]
    fn experiment() {
        {

            let mut fd = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open("./experiment.db").expect("文件打开 or 创建  失败");
            let mut data: [u8; 16384] = [0; 16384];
            let value = ValueTest {
                id: 16,
                data: String::from("asadfoqnljasdfjoij"),
            };
            let mut encode_value_u8: Vec<u8> = vec![];
            println!("{:?}", value.encode(&mut encode_value_u8));
            let value_len: u64 = encode_value_u8.len() as u64;
            let mut wtr: Vec<u8> = vec![];
            wtr.write_u64::<BigEndian>(value_len).unwrap();
            //写入数据
            println!("{:?}", fd.seek(SeekFrom::Start(0)));
            fd.write(&wtr).unwrap();
            fd.write(&encode_value_u8).unwrap();
            //读取数据
            let mut data: [u8; 16384] = [0; 16384];
            println!("{:?}", fd.seek(SeekFrom::Start(0)));
            fd.read(&mut data).unwrap();
            let data_len = Cursor::new(&data[0..8]).read_u64::<BigEndian>().unwrap();
            println!("read data {:?}", data_len);
            println!("{:?}", ValueTest::decode(&data[8..=data_len as usize]).unwrap());
            fd.flush().unwrap();
        }


        {
            static COUNTER: AtomicUsize = AtomicUsize::new(1);

            use std::{
                sync::atomic::{AtomicUsize, Ordering},
                thread,
            };
            let t1 = thread::spawn(move || {
                COUNTER.fetch_add(10, Ordering::SeqCst);
            });

            let t2 = thread::spawn(move || {
                COUNTER
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| Some(v * 10))
                    .unwrap();
            });

            t2.join().unwrap();
            t1.join().unwrap();

            println!("COUNTER: {}", COUNTER.load(Ordering::Relaxed));
        }
    }
}

