use std::fmt::Debug;
use std::io::{Cursor};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::{DecodableU8, EncodableU8, Size};
use thiserror::Error;
use anyhow::Result;

#[derive(Error, Debug)]
pub enum BPlusError {
    #[error("node error")]
    NodeError(String),
}

use std::convert::From;



impl From<std::io::Error> for BPlusError{
    fn from(item:std::io::Error) -> Self{
        Self::NodeError(item.to_string())
    }
}

// From<std::io::Error>
/// 是 root 节点也有可能是叶子节点(初始状态)
//无效空闲列表
const INVALID: u8 = 0b00000000;
//有效位
const VALID: u8 = 0b00000001;
const ROOT: u8 = 0b00000010;
//中间节点
const MIDDLE_NODE: u8 = 0b00000100;
//叶子
const LEAF: u8 = 0b00001000;
//额外数据页
const EXTRA_DATA: u8 = 0b00010000;

static PAGE_SIZE: AtomicUsize = AtomicUsize::new(16 * 1024);
static MAX_KEY: AtomicUsize = AtomicUsize::new(3);

#[derive(Debug)]
struct Node<K, V> {
    flag: u8,
    is_change: bool,
    // 空间换时间
    key: Option<Vec<Box<K>>>,
    key_seek: Option<Vec<u64>>,
    // 空间换时间
    value: Option<Vec<Box<V>>>,
    //可能是key 或者 data(value)
    u8: Option<Vec<u8>>,
    seek_start: u64,
    key_count: u64,
    data_count: u64,
    residual_storage_size: u64,
    next: u64,
    prev: u64,
    _k: PhantomData<K>,
    // key value 需要固定泛型
    _v: PhantomData<V>,
}

impl<K, V> Node<K, V> where
    K: EncodableU8 + DecodableU8 + Size + PartialEq + PartialOrd + Debug + Clone + Send + Sync,
    V: EncodableU8 + DecodableU8 + Debug + Clone + Send + Sync
{
    fn stop(&self) -> Result<Vec<u8>> {
        let max_page_size = PAGE_SIZE.load(Ordering::Relaxed);
        let mut data: Vec<u8> = Vec::with_capacity(max_page_size);
        let mut wtr: Vec<u8> = vec![];
        //写入flag
        data.push(self.flag);

        //写入key个数
        wtr.write_u64::<BigEndian>(self.key_count)?;
        //底层调用 set_len  wtr 变成长度为0
        data.append(&mut wtr);

        //写入数据个数
        wtr.write_u64::<BigEndian>(self.data_count)?;
        data.append(&mut wtr);

        //可存储数据大小 占位
        let residual_storage_size_index = data.len();
        wtr.write_u64::<BigEndian>(self.residual_storage_size)?;
        data.append(&mut wtr);

        //写上一个节点位置
        wtr.write_u64::<BigEndian>(self.prev)?;
        data.append(&mut wtr);

        //写下一个节点位置
        wtr.write_u64::<BigEndian>(self.next)?;
        data.append(&mut wtr);

        if (self.flag & MIDDLE_NODE) == MIDDLE_NODE {
            let mut data_u8 = self.key_encode()?;
            data.append(&mut data_u8);
        } else if (self.flag & LEAF) == LEAF {} else {}
        if data.len() < max_page_size {
            unsafe {
                data.set_len(max_page_size);
            }
        }
        Ok(data)
    }

    fn new_node_from_byte(seek: u64, data: Vec<u8>) -> Result<Self> {
        let mut node_data = Node::<K, V> {
            flag: data[0],
            is_change: false,
            key: None,
            key_seek: None,
            value: None,
            u8: None,
            seek_start: seek,
            key_count: 0,
            data_count: 0,
            residual_storage_size: 0,
            next: 0,
            prev: 0,
            _k: PhantomData,
            _v: PhantomData,
        };
        if (node_data.flag & VALID)!=VALID {
            return Ok(node_data);
        }
        // key_count data_count residual_storage_size prev next
        node_data.key_count = Cursor::new(&data[1..9]).read_u64::<BigEndian>()?;
        node_data.data_count = Cursor::new(&data[9..17]).read_u64::<BigEndian>()?;
        node_data.residual_storage_size = Cursor::new(&data[17..25]).read_u64::<BigEndian>()?;
        node_data.prev = Cursor::new(&data[25..33]).read_u64::<BigEndian>()?;
        node_data.next = Cursor::new(&data[33..41]).read_u64::<BigEndian>()?;

        if (node_data.flag & EXTRA_DATA)==EXTRA_DATA {
            //todo:未完成
            return Ok(node_data);
        }
        if( node_data.flag & MIDDLE_NODE)==MIDDLE_NODE {
            node_data.u8 = Some(data);
            node_data.key_decode()?;
            return Ok(node_data);
        }
        if (node_data.flag & LEAF)==LEAF {}
        if (node_data.flag & ROOT)==ROOT {}
        Ok(node_data)
    }
    fn key_decode(&mut self) -> Result<()> {
        if let Some(b) = &self.u8 {
            if self.key_count > 0 {
                let mut i: u64 = 0;
                let mut seek = 41;
                let key_size = K::size() as usize;
                let mut key_seek = Vec::with_capacity((self.key_count + 1) as usize);
                let mut key: Vec<Box<K>> = Vec::with_capacity(self.key_count as usize);
                while self.key_count > i {
                    key_seek.push(Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?);
                    seek += 8;
                    key.push(Box::new(K::decode(&b[seek..seek + key_size])?.0));
                    seek += key_size;
                    i += 1;
                }
                key_seek.push(Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?);
                self.key = Some(key);
                self.key_seek = Some(key_seek);
            }
        }
        Ok(())
    }
    fn key_encode(&self) -> Result<Vec<u8>,BPlusError> {
        if let (Some(key), Some(index)) = (&self.key, &self.key_seek) {
            if key.len() > 0 {
                //偏移固定 u64 大小
                let mut key_u8: Vec<u8> = Vec::with_capacity(((K::size() + 8) * self.key_count + 8) as usize);
                let mut key_encode: Vec<u8> = Vec::with_capacity(K::size() as usize);
                let mut index_seek = vec![];
                for (k, v) in key.iter().enumerate() {
                    //index seek
                    let s = index[k];
                    index_seek.write_u64::<BigEndian>(s)?;
                    key_u8.append(&mut index_seek);
                    //key
                    v.encode(&mut key_encode)?;
                    key_u8.append(&mut key_encode)
                }
                index_seek.write_u64::<BigEndian>(index[key.len()])?;
                key_u8.append(&mut index_seek);
                return Ok(key_u8);
            }
        }
       Err( BPlusError::NodeError("asdas".to_string()))
    }
    // fn value_decode();
    // fn extra_data_decode();
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::marker::PhantomData;
    use crate::node::node::{MIDDLE_NODE, Node, VALID};


    #[test]
    fn node_key() {
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("./experiment1.db").expect("文件打开 or 创建  失败");
        let  key: Vec<Box<u64>> = vec![Box::new(1), Box::new(2)];
        let mut node = Node::<u64, u64> {
            flag: MIDDLE_NODE|VALID,
            is_change: true,
            key: Some(key),
            key_seek: Some(vec![16384, 32768, 49152]),
            value: None,
            u8: None,
            seek_start: 0,
            key_count: 2,
            data_count: 0,
            residual_storage_size: 0,
            next: 0,
            prev: 0,
            _k: PhantomData,
            _v: PhantomData,
        };
        let mut u8data = node.stop().unwrap();
        fd.seek(SeekFrom::Start(node.seek_start)).unwrap();
        fd.write(&u8data).unwrap();
    }
    #[test]
    fn key_encode(){
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("./experiment1.db").expect("文件打开 or 创建  失败");
        let mut data = Vec::with_capacity(16384);
        unsafe {
            data.set_len(16384);
        }
        fd.seek(SeekFrom::Start(0)).unwrap();
        fd.read(&mut data).unwrap();
        let node = Node::<u64,u64>::new_node_from_byte(0,data).unwrap();
        println!("{:?}",node)
    }
}