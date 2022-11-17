use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::{DecodableU8, EncodableU8, Size};
use thiserror::Error;
use anyhow::Result;
use std::convert::From;
use std::io::Cursor;


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
const NODE_FIXED_SIZE: usize = 41;

static PAGE_SIZE: AtomicUsize = AtomicUsize::new(16 * 1024);
static MAX_KEY: AtomicUsize = AtomicUsize::new(3);
static DATA_LENGTH: AtomicUsize = AtomicUsize::new(256);

#[derive(Debug)]
pub struct ExtraData {
    pub seek: u64,
    pub data: Option<Vec<u8>>,
    pub next: Option<Vec<ExtraData>>,
}

impl ExtraData {
    pub(crate) fn data_extra_decode(b: &Vec<u8>, seek_index: u64) -> Result<(ExtraData, u64)> {
        let mut seek = NODE_FIXED_SIZE;
        let mut result = ExtraData {
            seek: seek_index,
            data: None,
            next: None,
        };
        let extra_origin_length = Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?;
        let extra_len = Cursor::new(&b[seek + 8..seek + 16]).read_u64::<BigEndian>()?;
        seek += 16;
        if extra_origin_length > extra_len {
            result.data = Some(b[seek..seek + extra_origin_length].to_vec());
            return Ok((result, Cursor::new(&b[seek + extra_origin_length..seek + extra_origin_length + 8]).read_u64::<BigEndian>()?));
        }
        result.data = Some(b[seek..seek + extra_origin_length].to_vec());
        Ok((result, 0))
    }
}

#[derive(Debug)]
struct Node<K, V> {
    flag: u8,
    is_change: bool,
    // 空间换时间
    key: Option<Vec<Box<K>>>,
    key_seek: Option<Vec<u64>>,
    // 空间换时间
    value: Option<Vec<Box<V>>>,
    pub extra_data: Option<Vec<Option<ExtraData>>>,
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
    //new_node_from_byte u8转换成node
    pub(crate) fn new_node_from_byte(seek: u64, data: Vec<u8>) -> Result<Self> {
        let mut node_data = Node::<K, V>::default();
        node_data.flag = data[0];

        if (node_data.flag & VALID) != VALID {
            return Ok(node_data);
        }
        // key_count data_count residual_storage_size prev next
        node_data.key_count = Cursor::new(&data[1..9]).read_u64::<BigEndian>()?;
        node_data.data_count = Cursor::new(&data[9..17]).read_u64::<BigEndian>()?;
        node_data.residual_storage_size = Cursor::new(&data[17..25]).read_u64::<BigEndian>()?;
        node_data.prev = Cursor::new(&data[25..33]).read_u64::<BigEndian>()?;
        node_data.next = Cursor::new(&data[33..41]).read_u64::<BigEndian>()?;
        node_data.seek_start = seek;

        if (node_data.flag & EXTRA_DATA) == EXTRA_DATA {
            //todo:未完成
            return Ok(node_data);
        }
        if (node_data.flag & MIDDLE_NODE) == MIDDLE_NODE {
            node_data.key_decode(&data)?;
            return Ok(node_data);
        }
        if (node_data.flag & LEAF) == LEAF {
            node_data.data_decode(&data)?
        }

        Ok(node_data)
    }

    pub(crate) fn stop(&self) -> Result<Vec<u8>, BPlusError> {
        let max_page_size = PAGE_SIZE.load(Ordering::Relaxed);
        let mut data: Vec<u8> = Vec::with_capacity(max_page_size);
        let mut wtr: Vec<u8> = vec![];
        //写入flag  1
        data.push(self.flag);

        //写入key个数  8
        wtr.write_u64::<BigEndian>(self.key_count)?;
        //底层调用 set_len  wtr 变成长度为0
        data.append(&mut wtr);

        //写入数据个数  8
        wtr.write_u64::<BigEndian>(self.data_count)?;
        data.append(&mut wtr);

        //可存储数据大小 占位  8
        let residual_storage_size_index = data.len();
        wtr.write_u64::<BigEndian>(self.residual_storage_size)?;
        data.append(&mut wtr);

        //写上一个节点位置  8
        wtr.write_u64::<BigEndian>(self.prev)?;
        data.append(&mut wtr);

        //写下一个节点位置  8  41
        wtr.write_u64::<BigEndian>(self.next)?;
        data.append(&mut wtr);

        if (self.flag & MIDDLE_NODE) == MIDDLE_NODE {
            let mut data_u8 = self.key_encode()?;
            data.append(&mut data_u8);
            if data.len() < max_page_size {
                unsafe {
                    data.set_len(max_page_size);
                }
            }
        } else if (self.flag & LEAF) == LEAF {
            let mut data_u8 = self.data_encode()?;
            data.append(&mut data_u8);
            //剩余数据容量
            wtr.write_u64::<BigEndian>((max_page_size - data.len()) as u64)?;
            data[residual_storage_size_index - 1..residual_storage_size_index + 7] = wtr[0..8]
        }

        Ok(data)
    }

    //data_encode 进行编码
    pub(crate) fn data_encode(&self) -> Result<Vec<u8>, BPlusError> {
        //16是长度固定大小
        let data_max_len = DATA_LENGTH.load(Ordering::Relaxed) as u64 - K::size() - 16;
        let max_page_size = PAGE_SIZE.load(Ordering::Relaxed) - NODE_FIXED_SIZE;
        let mut data_u8: Vec<u8> = Vec::with_capacity(max_page_size);
        let key = &self.key.as_ref().ok_or(BPlusError::NodeError("not key".to_string()))?;
        if let Some(data) = &self.value {
            let mut data_encode: Vec<u8> = Vec::with_capacity(data_max_len as usize);
            let mut key_encode: Vec<u8> = Vec::with_capacity(K::size() as usize);
            let mut temp: Vec<u8> = Vec::with_capacity(8);
            for (i, v) in data.iter().enumerate() {
                let encode_len = v.encode(&mut data_encode)?;
                //写入原始长度
                temp.write_u64::<BigEndian>(encode_len)?;
                data_u8.append(&mut temp);

                //可变长度显示
                if encode_len > data_max_len {
                    if let Some(extra_vec) = &self.extra_data {
                        if let Some(extra) = &extra_vec[i] {
                            //写入保存长度
                            temp.write_u64::<BigEndian>(data_max_len)?;
                            data_u8.append(&mut temp);
                            //写入key
                            key[i].encode(&mut key_encode)?;
                            data_u8.append(&mut key_encode);

                            // data
                            v.encode(&mut data_encode)?;
                            data_u8.append(&mut data_encode[0..data_max_len - 8]);

                            temp.write_u64::<BigEndian>(extra.seek)?;
                            data_u8.append(&mut temp);
                        }
                    } else {
                        //没有额外数据页
                        return Err(BPlusError::MissingExtraData());
                    }
                } else {
                    //实际长度
                    temp.write_u64::<BigEndian>(encode_len)?;
                    data_u8.append(&mut temp);
                    //写入key
                    key[i].encode(&mut key_encode)?;
                    data_u8.append(&mut key_encode);
                    //写入数据
                    data_u8.append(&mut data_encode);
                }
            }
            if data_u8.len() > max_page_size {
                return Err(BPlusError::PageMax());
            }
            return Ok(data_u8);
        }
        Err(BPlusError::NodeError("not key".to_string()))
    }

    //data_decode
    pub(crate) fn data_decode(&mut self, b: &Vec<u8>) -> Result<()> {
        if self.data_count > 0 {
            let mut i: usize = 0;
            let data_count: usize = self.data_count as usize;
            let mut seek = NODE_FIXED_SIZE;
            let key_size = K::size() as usize;
            let node_data_extra_length = DATA_LENGTH.load(Ordering::Relaxed) - (8 + 8 + key_size) - 8;
            let node_data_not_extra_length = DATA_LENGTH.load(Ordering::Relaxed) - (8 + 8 + key_size);
            let mut value: Vec<Box<V>> = Vec::with_capacity(self.key_count as usize);
            let mut data_decode_vec: Vec<u8> = Vec::with_capacity(DATA_LENGTH.load(Ordering::Relaxed));
            if let Some(extra) = &self.extra_data {
                while i < data_count {
                    let data_origin_length = Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?;
                    seek += 8;
                    let data_length = Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?;
                    seek += 8;
                    //key值
                    seek += key_size;
                    //只是写入 额外数据页信息
                    if data_origin_length > data_length {
                        data_decode_vec.extend_from_slice(&b[seek..seek + node_data_extra_length]);
                        if let Some(extra_data) = &extra[i] {
                            data_decode_vec.append(&mut join_extra(extra_data));
                        }
                        value.push(Box::new(V::decode(&data_decode_vec)?.0));
                        seek += node_data_not_extra_length;
                    } else {
                        data_decode_vec.extend_from_slice(&b[seek..seek + data_length]);
                        value.push(Box::new(V::decode(&data_decode_vec)?.0));
                        seek += data_length;
                    }
                    unsafe {
                        data_decode_vec.set_len(0)
                    }
                    i += 1;
                }
            }
        }

        Ok(())
    }


    //data_decode 因为有可变长数据的存在，只是解析出是否有可变长的额外数据
    pub(crate) fn data_decode_init(&mut self, b: &Vec<u8>) -> Result<()> {
        if self.data_count > 0 {
            let mut i: u64 = 0;
            let mut seek = NODE_FIXED_SIZE;
            let key_size = K::size() as usize;
            let node_data_length = DATA_LENGTH.load(Ordering::Relaxed) - (8 + 8 + key_size) - 8;
            let mut key: Vec<Box<K>> = Vec::with_capacity(self.key_count as usize);
            let mut extra_data: Vec<Option<ExtraData>> = Vec::with_capacity(self.key_count as usize);
            //todo:这里需要一个标记是否有额外页进行优化
            while self.data_count > i {
                let data_origin_length = Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?;
                seek += 8;
                let data_length = Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?;
                seek += 8;
                //key值
                key.push(Box::new(K::decode(&b[seek..seek + key_size])?.0));
                seek += key_size;

                //只是写入 额外数据页信息
                if data_origin_length > data_length {
                    seek += node_data_length;
                    extra_data.push(Some(ExtraData {
                        seek: Cursor::new(&b[seek..seek + 8]).read_u64::<BigEndian>()?,
                        data: None,
                        next: None,
                    }));
                    seek += 8;
                } else {
                    seek += data_length;
                    extra_data.push(None)
                }
                i += 1;
            }
            self.key = Some(key);
            self.key_seek = Some(key_seek);
            self.extra_data = Some(extra_data);
        }
        Ok(())
    }


    //key_decode key 编码处理
    pub(crate) fn key_decode(&mut self, b: &Vec<u8>) -> Result<()> {
        if self.key_count > 0 {
            let mut i: u64 = 0;
            let mut seek = NODE_FIXED_SIZE;
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
        Ok(())
    }

    //key_encode key 转换u8
    pub(crate) fn key_encode(&self) -> Result<Vec<u8>, BPlusError> {
        if let (Some(key), Some(index)) = (&self.key, &self.key_seek) {
            if key.len() > 0 {
                //偏移固定 u64 大小
                let mut key_u8: Vec<u8> = Vec::with_capacity(((K::size() + 8) * &self.key_count + 8) as usize);
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
        Err(BPlusError::NodeError("not key".to_string()))
    }


// fn value_decode();
// fn extra_data_decode();
}

fn join_extra(extra: &ExtraData) -> Vec<u8> {
    let mut extra_data: Vec<u8> = Vec::new();
    if let Some(data) = &extra.data {
        extra_data.extend_from_slice(&data);
    }
    if let Some(data) = &extra.next {
        for v in data.iter() {
            if let Some(extra) = &v.data {
                extra_data.extend_from_slice(extra);
            }
        }
    }
    extra_data
}

impl<K, V> Default for Node<K, V> {
    fn default() -> Self {
        Node::<K, V> {
            flag: INVALID,
            is_change: false,
            key: None,
            key_seek: None,
            value: None,
            extra_data: None,
            seek_start: 0,
            key_count: 0,
            data_count: 0,
            residual_storage_size: 0,
            next: 0,
            prev: 0,
            _k: PhantomData,
            _v: PhantomData,
        }
    }
}

#[derive(Error, Debug)]
pub enum BPlusError {
    #[error("node error")]
    NodeError(String),
    #[error("missing extra data error")]
    MissingExtraData(),
    #[error("page max error")]
    PageMax(),
}


impl From<std::io::Error> for BPlusError {
    fn from(item: std::io::Error) -> Self {
        Self::NodeError(item.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::marker::PhantomData;
    use crate::node::node::{LEAF, MIDDLE_NODE, Node, VALID};

    #[test]
    fn data_encode() {
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("./data.db").expect("文件打开 or 创建  失败");
        let mut data = Vec::with_capacity(16384);
        unsafe {
            data.set_len(16384);
        }
        fd.seek(SeekFrom::Start(0)).unwrap();
        fd.read(&mut data).unwrap();
        let node = Node::<u64, u64>::new_node_from_byte(0, data).unwrap();
        println!("{:?}", node)
    }

    #[test]
    fn data_decode() {
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("./data.db").expect("文件打开 or 创建  失败");
        let key: Vec<Box<u64>> = vec![Box::new(1), Box::new(2), Box::new(3), Box::new(4)];
        let node = Node::<u64, u64> {
            flag: LEAF | VALID,
            is_change: true,
            key: Some(key),
            key_seek: None,
            value: Some(vec![Box::new(1), Box::new(2), Box::new(3), Box::new(4)]),
            extra_data: None,
            seek_start: 0,
            key_count: 4,
            data_count: 4,
            residual_storage_size: 0,
            next: 0,
            prev: 0,
            _k: PhantomData,
            _v: PhantomData,
        };
        let u8data = node.stop().unwrap();
        fd.seek(SeekFrom::Start(node.seek_start)).unwrap();
        fd.write(&u8data).unwrap();
    }

    #[test]
    fn node_key() {
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("./key.db").expect("文件打开 or 创建  失败");
        let key: Vec<Box<u64>> = vec![Box::new(3), Box::new(4)];
        let node = Node::<u64, u64> {
            flag: MIDDLE_NODE | VALID,
            is_change: true,
            key: Some(key),
            key_seek: Some(vec![16384, 32768, 49152]),
            value: None,
            extra_data: None,
            seek_start: 0,
            key_count: 2,
            data_count: 0,
            residual_storage_size: 0,
            next: 0,
            prev: 0,
            _k: PhantomData,
            _v: PhantomData,
        };
        let u8data = node.stop().unwrap();
        fd.seek(SeekFrom::Start(node.seek_start)).unwrap();
        fd.write(&u8data).unwrap();
    }

    #[test]
    fn key_encode() {
        let mut fd = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open("./key.db").expect("文件打开 or 创建  失败");
        let mut data = Vec::with_capacity(16384);
        unsafe {
            data.set_len(16384);
        }
        fd.seek(SeekFrom::Start(0)).unwrap();
        fd.read(&mut data).unwrap();
        let node = Node::<u64, u64>::new_node_from_byte(0, data).unwrap();
        println!("{:?}", node)
    }
}